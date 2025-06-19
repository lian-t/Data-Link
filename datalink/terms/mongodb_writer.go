package terms

import (
	"context"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"encoding/json"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"time"
)

type MongoDBWriter struct {
	WriterBaseParams                    // 继承了 Writer
	client           *mongo.Client      // mongodb客户端
	cancel           context.CancelFunc // 取消
	ctx              context.Context    // context
	latestConnTime   time.Time          // 上一次连接时间
}

// NewMongoDBWriter 写入对象
func NewMongoDBWriter(id int, tid string, rc conf.Resource, tc conf.Writer) *MongoDBWriter {
	writer := new(MongoDBWriter)
	writer.Id = id
	writer.TaskId = tid
	writer.TargetConf = tc
	writer.DocumentSetMap = tc.DocumentSet
	writer.ResourceConf = rc
	return writer
}

// clientInstance 获取实例
func (_this *MongoDBWriter) clientInstance(flush bool) (*mongo.Client, error) {
	cc := _this.client
	if cc == nil {
		flush = true
	}
	if !flush {
		if time.Now().Sub(_this.latestConnTime).Hours() >= 4 {
			flush = true
		}
	}
	var err error
	if flush {
		if cc == nil {
			cc, err = MongoDBConn(_this.ResourceConf)
		} else {
			err = cc.Ping(context.Background(), nil)
		}
		// 重试一次
		if err != nil {
			cc, err = MongoDBConn(_this.ResourceConf)
			if err != nil {
				return nil, err
			}
		}
		_this.latestConnTime = time.Now()
		_this.client = cc
	}
	return cc, err
}

// Run 写入目标数据,单次写入最长时间为1分钟
func (_this *MongoDBWriter) Run(ch chan []*msg.Op, errC chan error, call func(int)) {
	_this.errC = errC
	_this.exitWG.Add(1)

	// 设置limit时,则开启批量写入. 最长5秒写入一次
	limitValue, _ := _this.TargetConf.Extra["limit"]
	limitF, _ := limitValue.(float64)
	limit := int(limitF)
	if limit < 1 {
		limit = 1
	}

	var needFlush bool
	var cacheList []*msg.Op // 缓存队列
	timeout := time.Second * 30
	t := time.NewTimer(timeout)
	for {
		if _this.exitF {
			if len(cacheList) >= 0 {
				_this.Write(cacheList)
			}
			break
		}

		select {
		case ops := <-ch:
			// 结束命令
			if len(ops) == 1 && ops[0].MessageType == msg.MessageTypeCmd {
				op := ops[0]
				switch op.Cmd.Type {
				case msg.CmdTypeReadDone:
					v, ok := op.Cmd.Value.(bool)
					if ok && v == true {
						_this.exitF = true
					}
				}
				continue
			}
			cacheList = append(cacheList, ops...)
			// 计算是否需要执行写入
			if len(cacheList) >= limit {
				needFlush = true
			}
		case <-t.C:
			// 计算是否需要执行写入
			// 为了查看跳出
			if len(cacheList) >= 0 {
				needFlush = true
			}
		}

		if !needFlush {
			continue
		}

		// 设置写入超时
		failed := 0
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		go func(ctx context.Context) {
			failed = _this.Write(cacheList)
			cancel()
		}(ctx)
		select {
		case <-ctx.Done():
			call(failed)
		case <-time.After(timeout):
			call(len(cacheList))
			cancel()
			_this.errC <- errors.New("taskId:" + _this.TaskId + ", timeout write")
			for _, op := range cacheList {
				msg, _ := json.Marshal(op.Doc)
				_this.errC <- errors.New(string(msg))
			}
		}

		// reset
		cacheList = cacheList[:0]
		needFlush = false
	}
	_this.exitWG.Done()
	_this.Release()
	_this.errC = nil
}

// Stop 停止
func (_this *MongoDBWriter) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Release 断开连接
func (_this *MongoDBWriter) Release() {
	log.Info("mongodb target write stop")
	if _this.client != nil {
		_this.client.Disconnect(context.Background())
		_this.client = nil
	}
	_this.DocumentSetMap = nil
}

// Write 写入数据
func (_this *MongoDBWriter) Write(ops []*msg.Op) (failed int) {

	instance, err := _this.clientInstance(false)
	if err != nil {
		_this.errC <- err
		return len(ops)
	}

	// 分组
	insertOpsMap := map[string][]*msg.Op{}
	updateOpsMap := map[string][]*msg.Op{}
	deleteOpsMap := map[string][]*msg.Op{}

	// 先将 collection 分组
	for _, op := range ops {
		ns, ok := _this.DocumentSetMap[op.DocumentSet]
		if !ok {
			_this.errC <- errors.New("mongodb need DocumentSet")
			continue
		}

		// 操作分组
		switch op.OptionType {
		case msg.OptionTypeInsert:
			l, ok := insertOpsMap[ns]
			if !ok {
				insertOpsMap[ns] = []*msg.Op{}
			}
			l = append(l, op)
			insertOpsMap[ns] = l
		case msg.OptionTypeUpdate:
			l, ok := updateOpsMap[ns]
			if !ok {
				updateOpsMap[ns] = []*msg.Op{}
			}
			l = append(l, op)
			updateOpsMap[ns] = l
		case msg.OptionTypeDelete:
			l, ok := deleteOpsMap[ns]
			if !ok {
				deleteOpsMap[ns] = []*msg.Op{}
			}
			l = append(l, op)
			deleteOpsMap[ns] = l
		}
	}

	// insert
	for docSetStr, groupOps := range insertOpsMap {
		docSetArr := strings.Split(docSetStr, ".")
		if len(docSetArr) != 2 {
			_this.errC <- errors.New("mongodb ns format error:" + docSetStr)
			continue
		}
		db := docSetArr[0]
		col := docSetArr[1]
		coll := instance.Database(db).Collection(col)
		opts := options.InsertMany().SetOrdered(false)

		var docs []interface{}
		for _, op := range groupOps {
			doc := op.Doc
			if op.DocIdField != "" && op.DocIdValue != "" {
				delete(doc, "_id")
				_id, err := primitive.ObjectIDFromHex(op.DocIdValue)
				if err != nil {
					doc["_id"] = op.DocIdValue
				} else {
					doc["_id"] = _id
				}
			}
			docs = append(docs, doc)
		}
		_, err = coll.InsertMany(context.Background(), docs, opts)
		if err != nil {
			_this.errC <- err
			failed += len(docs)
			continue
		}
	}

	// update
	for docSetStr, groupOps := range updateOpsMap {
		docSetArr := strings.Split(docSetStr, ".")
		if len(docSetArr) != 2 {
			_this.errC <- errors.New("mongodb ns format error:" + docSetStr)
			continue
		}
		db := docSetArr[0]
		col := docSetArr[1]
		coll := _this.client.Database(db).Collection(col)
		for _, op := range groupOps {
			doc := op.Doc
			opts := options.Update()
			_, err = coll.UpdateByID(context.Background(), op.DocIdValue, bson.M{"$set": doc}, opts)
			if err != nil {
				_this.errC <- err
				failed += 1
				continue
			}
		}
	}

	// delete
	for docSetStr, groupOps := range deleteOpsMap {
		docSetArr := strings.Split(docSetStr, ".")
		if len(docSetArr) != 2 {
			_this.errC <- errors.New("mongodb ns format error:" + docSetStr)
			continue
		}
		db := docSetArr[0]
		col := docSetArr[1]
		coll := _this.client.Database(db).Collection(col)
		opts := options.Delete()
		for _, op := range groupOps {
			_, err = coll.DeleteOne(context.Background(), bson.M{"_id": op.DocIdValue}, opts)
			if err != nil {
				_this.errC <- err
				failed += 1
				continue
			}
		}
	}
	return failed
}
