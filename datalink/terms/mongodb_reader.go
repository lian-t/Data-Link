package terms

import (
	"context"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/ext"
	"data-link-2.0/internal/helper"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"data-link-2.0/internal/x"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"time"
)

type MongoDBReader struct {
	ReaderBaseParams                     // 继承了 Reader
	client           *mongo.Client       // client
	stream           *mongo.ChangeStream // stream
	latestConnTime   time.Time           // 最近使用时间
	resumeKey        *ResumeKey          // 最近使用时间
}

// NewMongoDBReader 新建mongo的reader
func NewMongoDBReader(id int, tid string, rc conf.Resource, sc conf.Reader) *MongoDBReader {

	// 1.是否启用resume
	resumeFile := ""
	resume, ok := sc.Extra["resume"]
	if ok == true && resume == true {
		resumeFile = fmt.Sprintf("%s%s_%s_resume_file", conf.G.TaskResumePath, tid, rc.Id)
	}

	// 构建source
	reader := new(MongoDBReader)
	reader.Id = id
	reader.TaskId = tid
	reader.SourceConf = sc
	reader.ResourceConf = rc
	reader.ResumeFile = resumeFile
	reader.err = EmptyError
	return reader
}

// Run 运行任务
func (_this *MongoDBReader) Run(opC chan *msg.Op, errC chan error) {
	_this.errC = errC
	_this.exitWG.Add(1)
	x.GoSafe(func() {
		defer func() {
			_this.exitWG.Done()
		}()

		switch _this.SourceConf.SyncMode {
		case cst.SyncModeDump:
			_this.Dump(opC)
		case cst.SyncModeDirect:
			_this.Direct(opC)
		case cst.SyncModeStream:
			_this.Stream(opC)
		case cst.SyncModeReplica:
			_this.Replica(opC)
		case cst.SyncModeEmpty:
			// pass不需要释放对象,需要处理
		}
		_this.exitF = true
	})

	// 等待读取结束
	_this.exitWG.Wait()
}

// Stop 停止任务
func (_this *MongoDBReader) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Err 错误信息
func (_this *MongoDBReader) Err() error {
	if _this.err == EmptyError {
		return nil
	}
	return _this.err
}

// Clear 释放资源
func (_this *MongoDBReader) Clear() {
	_this.Release()
	_this.errC = nil
}

// Release 释放资源
func (_this *MongoDBReader) Release() {
	log.Info("mongodb read stop")
	if _this.client != nil {
		err := _this.client.Disconnect(context.Background())
		if err != nil {
			_this.errC <- err
		}
		_this.client = nil
	}
	if _this.stream != nil {
		_this.stream.Close(context.Background())
		_this.stream = nil
	}
}

// Dump 以dump方式导数据,依赖于源软件本身能力
func (_this *MongoDBReader) Dump(opC chan *msg.Op) {
	path := conf.G.MongoexportPath
	dsn := _this.ResourceConf.Dsn
	extra := _this.SourceConf.Extra

	var docSet string
	var docSetup conf.ReaderDocumentSet
	for docSet1, docSetup1 := range _this.SourceConf.DocumentSet {
		docSet = docSet1
		docSetup = docSetup1
		break
	}

	// dump 数据
	err := ext.MongoDump(path, dsn, docSet, func(bm *bson.M, err error) {
		if err != nil {
			_this.errC <- err
		}
		if _this.exitF {
			return
		}
		// 将ObjectId转为_id
		doc := map[string]interface{}{}
		var docId string
		for k, v := range *bm {
			if k == "_id" {
				switch v := v.(type) {
				case nil:
					docId = ""
				case primitive.ObjectID:
					docId = v.Hex()
				default:
					docId = fmt.Sprint(v)
				}
				doc[k] = docId
				continue
			}
			doc[k] = v
		}

		for _, alias := range docSetup.Alias {
			op := msg.NewDocOp(msg.OpSourceFmtNOSQL, docId, "_id", _this.TaskId, doc, nil)
			op.SourceId = _this.Id
			op.DocumentSet = docSet
			op.DocumentSetAlias = alias
			op.OptionType = msg.OptionTypeInsert
			opC <- op
		}
	}, extra)
	if err != nil {
		_this.err = err
	}
}

// Direct 以查表的方式遍历数据,提供特殊的查询方法,添加
func (_this *MongoDBReader) Direct(opC chan *msg.Op) {
	// 连接对象
	instance, err := _this.clientInstance(true)
	if err != nil {
		_this.err = err
		return
	}

	// collection
	for docSet, docSetAlias := range _this.SourceConf.DocumentSet {
		ns := strings.Split(docSet, ".")
		if len(ns) != 2 {
			_this.err = fmt.Errorf("documentSet format error:%s", docSet)
			return
		}
		coll := instance.Database(ns[0]).Collection(ns[1])

		var filter interface{}
		err = _this.ReadCollection(coll, filter, func(doc map[string]interface{}, opt string, tab string) {
			docId, _ := doc["_id"].(string)
			for _, alias := range docSetAlias.Alias {
				op := msg.NewDocOp(msg.OpSourceFmtNOSQL, docId, "_id", _this.TaskId, doc, nil)
				op.SourceId = _this.Id
				op.DocumentSet = tab
				op.DocumentSetAlias = alias
				op.OptionType = opt
				opC <- op
			}
		})

		if err != nil {
			if err == CancelError {
				return
			}
			_this.err = err
			return
		}
	}
}

// Stream 流读取
func (_this *MongoDBReader) Stream(opC chan *msg.Op) {
	// client
	instance, err := _this.clientInstance(true)
	if err != nil {
		_this.errC <- err
		return
	}

	// watch
	var dbName string
	var ca bson.A
	for docSet, _ := range _this.SourceConf.DocumentSet {
		ns := strings.Split(docSet, ".")
		if dbName == "" {
			dbName = ns[0]
		}
		if dbName != ns[0] {
			_this.errC <- fmt.Errorf("MongoDB Stream只支持监控一个数据库")
			return
		}
		for _, ev := range []string{"insert", "update", "delete", "replace"} {
			ca = append(ca, bson.M{"ns.db": ns[0], "ns.coll": ns[1], "operationType": ev})
		}
	}
	db := instance.Database(dbName)
	pipeline := mongo.Pipeline{bson.D{
		{"$match", bson.D{{"$or", ca}}},
	}}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	rk, _ := _this.InitResumeFile()
	if rk != nil {
		opts.SetStartAtOperationTime(&rk.resumeAt)
		_this.resumeKey = rk
		_this.timerSavePos()
	}
	err = _this.WatchFromDB(db, pipeline, opts, func(doc map[string]interface{}, opt string, tab string) {
		docId, _ := doc["_id"].(string)
		aliasList, _ := _this.SourceConf.DocumentSet[tab]
		for _, alias := range aliasList.Alias {
			op := msg.NewDocOp(msg.OpSourceFmtNOSQL, docId, "_id", _this.TaskId, doc, nil)
			op.SourceId = _this.Id
			op.DocumentSet = tab
			op.DocumentSetAlias = alias
			op.OptionType = opt
			opC <- op
		}
	})
	if err != nil {
		_this.errC <- err
	}
}

// Replica 副本处理
func (_this *MongoDBReader) Replica(opC chan *msg.Op) {
	// 连接
	instance, err := _this.clientInstance(true)
	if err != nil {
		_this.err = err
		return
	}

	// 检查 resume && watch
	rk, full := _this.InitResumeFile()
	if full {
		var ns []string
		for docSet, docSetAlias := range _this.SourceConf.DocumentSet {
			ns = strings.Split(docSet, ".")
			coll := instance.Database(ns[0]).Collection(ns[1])
			err := _this.ReadCollection(coll, nil, func(doc map[string]interface{}, opt string, tab string) {
				docId, _ := doc["_id"].(string)
				for _, alias := range docSetAlias.Alias {
					op := msg.NewDocOp(msg.OpSourceFmtNOSQL, docId, "_id", _this.TaskId, doc, nil)
					op.SourceId = _this.Id
					op.DocumentSet = tab
					op.DocumentSetAlias = alias
					op.OptionType = opt
					opC <- op
				}
			})
			if err != nil {
				_this.err = fmt.Errorf("Replica Read Collection Error: %s ", err.Error())
				return
			}
		}
	}

	// 检索是否能watch
	var dbName string
	var ca bson.A
	for docSet, _ := range _this.SourceConf.DocumentSet {
		ns := strings.Split(docSet, ".")
		if dbName == "" {
			dbName = ns[0]
		}
		if dbName != ns[0] {
			_this.errC <- fmt.Errorf("MongoDB Stream只支持监控一个数据库")
			return
		}
		for _, ev := range []string{"insert", "update", "delete", "replace"} {
			ca = append(ca, bson.M{"ns.db": ns[0], "ns.coll": ns[1], "operationType": ev})
		}
	}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if rk != nil {
		opts.SetStartAtOperationTime(&rk.resumeAt)
		_this.resumeKey = rk
		_this.timerSavePos()
	}
	pipeline := mongo.Pipeline{bson.D{
		{"$match", bson.D{{"$or", ca}}},
	}}
	db := instance.Database(dbName)
	err = _this.WatchFromDB(db, pipeline, opts, func(doc map[string]interface{}, opt string, tab string) {
		docId, _ := doc["_id"].(string)
		aliasList, _ := _this.SourceConf.DocumentSet[tab]
		for _, alias := range aliasList.Alias {
			op := msg.NewDocOp(msg.OpSourceFmtNOSQL, docId, "_id", _this.TaskId, doc, nil)
			op.SourceId = _this.Id
			op.DocumentSet = tab
			op.DocumentSetAlias = alias
			op.OptionType = opt
			opC <- op
		}
	})
	if err != nil {
		_this.errC <- err
	}
}

// SyncMode 同步模式
func (_this *MongoDBReader) SyncMode() string {
	return _this.SourceConf.SyncMode
}

// RelateOneByOne 一对一关联条件
//[
//	{"name": ["=", "sam"]},
//	{"name": ["=", "tom"]}
//]
func (_this *MongoDBReader) RelateOneByOne(docSet string, wheres []map[string][2]interface{}) (doc map[string]interface{}) {
	instance, err := _this.clientInstance(false)
	if err != nil {
		_this.errC <- err
		return nil
	}
	docSetSchema := strings.Split(docSet, ".")
	col := instance.Database(docSetSchema[0]).Collection(docSetSchema[1])

	var filter bson.D
	for _, whereItem := range wheres {
		for field, fieldItem := range whereItem {
			filter = append(filter, bson.E{Key: field, Value: fieldItem[1]})
		}
	}
	err = col.FindOne(context.Background(), filter).Decode(&doc)
	if err != nil {
		_this.errC <- err
		return nil
	}
	return doc
}

// RelateOneByMany 一对多关联关系
func (_this *MongoDBReader) RelateOneByMany(docSet string, wheres []map[string][2]interface{}) (docs []map[string]interface{}) {
	instance, err := _this.clientInstance(false)
	if err != nil {
		_this.errC <- err
		return nil
	}
	docSetSchema := strings.Split(docSet, ".")
	col := instance.Database(docSetSchema[0]).Collection(docSetSchema[1])

	var filter bson.D
	for _, whereItem := range wheres {
		for field, fieldItem := range whereItem {
			filter = append(filter, bson.E{Key: field, Value: fieldItem[1]})
		}
	}
	ctx := context.Background()
	cursor, err := col.Find(ctx, filter)
	if err != nil {
		return nil
	}
	for cursor.Next(ctx) {
		var doc map[string]interface{}
		if err = cursor.Decode(&doc); err != nil {
			_this.errC <- err
			continue
		}
		if len(doc) < 1 {
			continue
		}
		docs = append(docs, doc)
	}
	return docs
}

// RemoveResumeFile remove resume文件
func (_this *MongoDBReader) RemoveResumeFile() {
	// stream resume 文件
	if ok, _ := helper.PathExists(_this.ResumeFile); ok {
		os.Remove(_this.ResumeFile)
	}
}

// ReadCollection 读取集合数据
func (_this *MongoDBReader) ReadCollection(
	coll *mongo.Collection,
	filter interface{},
	item func(doc map[string]interface{}, op string, tab string),
) error {

	ctx := context.Background()
	opts := options.Find()
	opts.SetNoCursorTimeout(true)
	opts.SetBatchSize(100)
	opts.SetSort(bson.M{"_id": 1})

	if filter == nil {
		filter = bson.D{{}}
	}

	ns := fmt.Sprintf("%s.%s", coll.Database().Name(), coll.Name())
	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		if _this.exitF {
			_this.errC <- CancelError
			return CancelError
		}

		var bsonDoc bson.M
		if err = cursor.Decode(&bsonDoc); err != nil {
			_this.errC <- err
			continue
		}

		// 将ObjectId转为_id
		oriDoc := map[string]interface{}{}
		var docId string
		for k, v := range bsonDoc {
			if k == "_id" {
				switch v := v.(type) {
				case nil:
					docId = ""
				case primitive.ObjectID:
					docId = v.Hex()
				default:
					docId = fmt.Sprint(v)
				}
				oriDoc[k] = docId
				continue
			}
			oriDoc[k] = v
		}

		// 通知进度
		if item != nil {
			item(oriDoc, msg.OptionTypeInsert, ns)
		}
	}
	return nil
}

// WatchFromDB 监控一个数据库
func (_this *MongoDBReader) WatchFromDB(db *mongo.Database,
	pipeline mongo.Pipeline,
	opts *options.ChangeStreamOptions,
	item func(doc map[string]interface{}, op string, tab string)) error {

	ctx := context.Background()
	stream, err := db.Watch(ctx, pipeline, opts)
	if err != nil {
		_this.errC <- err
		return err
	}
	_this.stream = stream
	for {
		if _this.exitF {
			break
		}

		if !_this.stream.TryNext(ctx) {
			err := _this.stream.Err()
			if err != nil {
				_this.errC <- err
			}
			time.Sleep(time.Second)
			continue
		}

		var event streamEvent
		if err = _this.stream.Decode(&event); err != nil {
			_this.errC <- err
			continue
		}

		// 记录resume
		if _this.resumeKey != nil {
			_this.resumeKey.resumeAt = event.ClusterTime
		}
		docSet := event.docSet()

		optionType := event.OperationType
		if optionType == "replace" {
			optionType = msg.OptionTypeUpdate
		}

		doc := event.FullDocument
		docId := event.DocumentKeyId(nil)
		if docId == "" {
			docId = event.DocumentKeyId(doc)
		}
		// delete时doc是nil
		if len(doc) < 1 {
			doc = map[string]interface{}{}
		}
		doc["_id"] = docId

		if item != nil {
			item(doc, optionType, docSet)
		}
	}
	err = _this.stream.Err()
	return err
}

// WatchFromColl 转换event数据
func (_this *MongoDBReader) WatchFromColl(col *mongo.Collection, opC chan *msg.Op, startAt primitive.Timestamp) error {

	ctx := context.TODO()

	// 获取 watch_event 事件
	extra := _this.SourceConf.Extra
	var watchEvent []string
	we, _ := extra["watch_event"]
	weArr, ok := we.([]interface{})
	if ok {
		for _, weItem := range weArr {
			weValue, _ := weItem.(string)
			if weValue == "" {
				continue
			}
			watchEvent = append(watchEvent, weValue)
		}
	} else {
		watchEvent = []string{"insert", "update", "delete"}
	}
	// mongo中的update事件,自动开启replace
	for _, v := range watchEvent {
		if v == "update" {
			watchEvent = append(watchEvent, "replace")
			break
		}
	}

	var arr bson.A
	for _, v := range watchEvent {
		arr = append(arr, bson.D{{"operationType", v}})
	}
	pipeline := mongo.Pipeline{bson.D{{"$match", bson.D{{"$or", arr}}}}}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if startAt.IsZero() == false {
		opts.SetStartAtOperationTime(&startAt)
	}
	stream, err := col.Watch(ctx, pipeline, opts)
	if err != nil {
		_this.errC <- err
		return err
	}
	_this.stream = stream

	x.GoSafe(func() {
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for {
			select {
			case <-tk.C:
				if _this.exitF {
					_this.stream.Close(ctx)
					return
				}
			}
		}
	})

	// 开启监听
	for {
		if _this.exitF {
			break
		}

		if !_this.stream.TryNext(ctx) {
			time.Sleep(time.Second)
			continue
		}

		var event streamEvent
		if err = _this.stream.Decode(&event); err != nil {
			_this.errC <- err
			continue
		}

		// 记录resume
		if _this.resumeKey != nil {
			_this.resumeKey.resumeAt = event.ClusterTime
		}
		docSet := event.docSet()

		optionType := event.OperationType
		if optionType == "replace" {
			optionType = msg.OptionTypeUpdate
		}

		doc := event.FullDocument
		docId := event.DocumentKeyId(nil)
		if docId == "" {
			docId = event.DocumentKeyId(doc)
		}
		doc["_id"] = docId

		op := msg.NewDocOp(msg.OpSourceFmtNOSQL, docId, "_id", _this.TaskId, doc, nil)
		op.SourceId = _this.Id
		op.DocumentSet = docSet
		op.DocumentSetAlias = docSet
		op.OptionType = optionType
		opC <- op
	}
	err = _this.stream.Err()
	return err
}

// clientInstance 获取实例
func (_this *MongoDBReader) clientInstance(force bool) (*mongo.Client, error) {
	cc := _this.client
	if cc == nil {
		force = true
	}
	if !force {
		if time.Now().Sub(_this.latestConnTime).Hours() > 4 {
			force = true
		} else if rand.Int()/10000 == 0 {
			force = true
		}
	}

	var err error
	if force {
		if cc == nil {
			cc, err = MongoDBConn(_this.ResourceConf)
		} else {
			err = cc.Ping(context.Background(), nil)
		}

		// 重试
		if err != nil {
			cc, err = MongoDBConn(_this.ResourceConf)
			if err != nil {
				return nil, err
			}
		}
		_this.client = cc
	}
	return cc, nil
}

// InitResumeFile 获取 resume
func (_this *MongoDBReader) InitResumeFile() (*ResumeKey, bool) {
	// 0.检查文件
	if _this.ResumeFile == "" {
		return nil, true
	}
	var full bool
	exists, err := helper.PathExists(_this.ResumeFile)
	if err != nil || exists == false {
		full = true
	}
	rk := NewResumeKey(_this.ResumeFile)

	// 2.检查是否有效
	// 3.获取oplog中最老的时间和最新的时间
	oldTS := OpLogTimeStamp(_this.client, 1)
	newTS := OpLogTimeStamp(_this.client, -1)
	if rk.resumeAt.IsZero() {
		rk.resumeAt = newTS
		full = true
	} else {
		// 比最老的时间更老,使用最新的时间
		if primitive.CompareTimestamp(oldTS, rk.resumeAt) == 1 {
			rk.resumeAt = newTS
			full = true
		}
		// 比最新的时间,使用最新的时间
		if primitive.CompareTimestamp(newTS, rk.resumeAt) == -1 {
			rk.resumeAt = newTS
			full = true
		}
	}
	return rk, full
}

// timerSavePos 保存进度
func (_this *MongoDBReader) timerSavePos() {

	if _this.resumeKey == nil {
		return
	}

	x.GoSafe(func() {
		tk := time.NewTicker(5 * time.Second)
		defer func() {
			tk.Stop()
		}()
		for {
			if _this.exitF {
				return
			}
			select {
			case <-tk.C:
				_this.resumeKey.save()
			}
		}
	})
}

type ResumeKey struct {
	resumeAt primitive.Timestamp // 时间
	path     string              // 文件路径
}

// NewResumeKey 转换为pos位置
func NewResumeKey(path string) *ResumeKey {
	r := new(ResumeKey)
	r.path = path
	ok, err := helper.PathExists(path)
	if ok == false || err != nil {
		return r
	}
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return r
	}
	var ts map[string]interface{}
	err = json.Unmarshal(bytes, &ts)
	if err != nil || ts == nil {
		return r
	}
	r.resumeAt = primitive.Timestamp{
		T: uint32(ts["T"].(float64)), // T 是秒
		I: uint32(ts["I"].(float64)),
	}
	return r
}

// save 保存到文件
func (r *ResumeKey) save() error {
	bts, err := json.Marshal(map[string]interface{}{
		"T": r.resumeAt.T,
		"I": r.resumeAt.I,
	})
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(r.path, bts, 0644)
	if err != nil {
		return err
	}
	return nil
}

// getPos 转换为pos位置
func (r *ResumeKey) getKey() (ts primitive.Timestamp) {
	return r.resumeAt
}

// OpLogTimeStamp 取oplog时间戳
// sort=1 asc, sort=-1 desc
func OpLogTimeStamp(client *mongo.Client, sort int8) primitive.Timestamp {
	opts := options.FindOne().SetSort(bson.M{"$natural": sort})
	col := client.Database("local").Collection("oplog.rs")
	ret := col.FindOne(nil, bson.M{}, opts)
	var ts primitive.Timestamp
	v := map[string]interface{}{}
	if ret.Err() == nil {
		ret.Decode(v)
		ts, _ = v["ts"].(primitive.Timestamp)
	}
	if ts.T < 1 {
		n := time.Now()
		return primitive.Timestamp{
			T: uint32(n.Unix()), // T 是秒
			I: uint32(n.UnixNano() % 1e9),
		}
	}
	return ts
}

// streamEvent 时间解析
type streamEvent struct {
	EventId struct {
		Data string `bson:"_data"`
	} `bson:"_id"` //_id
	ClusterTime  primitive.Timestamp    `bson:"clusterTime"`  // 服务器时间
	DocumentKey  map[string]interface{} `bson:"documentKey"`  // 文档主键
	FullDocument map[string]interface{} `bson:"fullDocument"` // 完整文档
	Ns           struct {
		Coll string `bson:"coll"`
		Db   string `bson:"db"`
	} `bson:"ns"` // docSet
	OperationType     string `bson:"operationType"` // insert update delete replace
	UpdateDescription struct {
		RemovedFields []interface{}          `bson:"removedFields"`
		UpdatedFields map[string]interface{} `bson:"updatedFields"`
	} `bson:"updateDescription"` // 修改的字段
}

// docSet 拼接NS
func (_this *streamEvent) docSet() string {
	return fmt.Sprintf("%s.%s", _this.Ns.Db, _this.Ns.Coll)
}

// DocumentKeyId 获取文档ID
func (_this *streamEvent) DocumentKeyId(doc map[string]interface{}) string {
	if doc == nil {
		doc = _this.DocumentKey
	}
	_idValue, _ := doc["_id"]
	switch _id := _idValue.(type) {
	case primitive.ObjectID:
		return _id.Hex()
	case []uint8:
		return string(_id)
	case string:
		return _id
	case nil:
		return ""
	default:
		return fmt.Sprint(_id)
	}
}
