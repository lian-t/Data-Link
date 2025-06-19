package terms

import (
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/ext"
	"data-link-2.0/internal/helper"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"data-link-2.0/internal/x"
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/dump"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pingcap/errors"
	"github.com/xwb1989/sqlparser"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MySQLReader mysql的读取
type MySQLReader struct {
	ReaderBaseParams
	clientLink     *client.Conn               // MySQL客户端连接
	canalLink      *canal.Canal               // canal 客户端连接
	dumper         *dump.Dumper               // canal 中的dump对象
	canalCfg       *canal.Config              // 配置
	masterInfo     *MasterInfo                // masterInfo
	lock           sync.RWMutex               // 读写锁
	latestConnTime time.Time                  // 上次使用时间
	onDDL          bool                       // 表结构变更
	onDDLDocSet    map[string]map[string]bool // 表结构变更
	blHeader       bool                       // binlog 元数据
}

// NewMySQLReader 构建一个 mysql 连接
func NewMySQLReader(id int, tid string, rc conf.Resource, sc conf.Reader) *MySQLReader {

	reader := new(MySQLReader)
	reader.Id = id
	reader.TaskId = tid
	reader.SourceConf = sc
	reader.ResourceConf = rc
	reader.err = EmptyError
	reader.loadExtra()

	switch sc.SyncMode {
	case cst.SyncModeDump:
		path := conf.G.MysqldumpPath
		if path == "" {
			reader.err = errors.New("Mysqldump Path can't empty ")
			return nil
		}
		info, err := os.Stat(path)
		if err != nil {
			reader.err = err
			return nil
		}
		if info.IsDir() {
			reader.err = errors.New(path + "is Dir")
			return nil
		}
		// 构建dump,需要使用mysqldump程序
		addr := fmt.Sprintf("%s:%s", rc.Host, rc.Port)
		cfg := canal.NewDefaultConfig()
		cfg.Addr = addr
		cfg.User = rc.User
		cfg.Password = rc.Pass
		//cfg.Flavor = mysql.MariaDBFlavor
		cfg.Flavor = mysql.MySQLFlavor
		cfg.Dump.ExecutionPath = conf.G.MysqldumpPath
		reader.canalCfg = cfg
	case cst.SyncModeDirect, cst.SyncModeStream, cst.SyncModeReplica, cst.SyncModeEmpty:
		// 构建stream配置,在运行时构建canal对象
		log.Info("NewMySQLReader cst.SyncModeEmpty")
	}

	return reader
}

// loadExtra 加载附加配置
func (_this *MySQLReader) loadExtra() bool {
	sce := _this.SourceConf.Extra

	// 1.加载binlog文件
	resumeValue, _ := sce["resume"]
	resume, _ := resumeValue.(bool)
	if resume {
		val, _ := sce["cdc_type"]
		cdcType, _ := val.(string)
		if cdcType == "field" {
			_this.ResumeFile = fmt.Sprintf("%s%s_%s_field_time", conf.G.TaskResumePath, _this.TaskId, _this.ResourceConf.Id)
		} else {
			_this.ResumeFile = fmt.Sprintf("%s%s_%s_masterinfo", conf.G.TaskResumePath, _this.TaskId, _this.ResourceConf.Id)
			_this.masterInfo = NewMasterInfo(_this.ResumeFile)
		}

	}

	// 2.监听修改表的事件
	tableChange, _ := sce["onDDL"]
	onDDL, _ := tableChange.(bool)
	if onDDL {
		_this.onDDL = onDDL
		_this.onDDLDocSet = map[string]map[string]bool{}
	}

	// 3.binlog源信息
	blHeader, _ := sce["blHeader"]
	_this.blHeader, _ = blHeader.(bool)

	return true
}

// clientInstance 获取client
func (_this *MySQLReader) clientInstance(force bool) (*client.Conn, error) {
	cc := _this.clientLink
	if cc == nil {
		force = true
	}
	if !force {
		if time.Now().Sub(_this.latestConnTime).Hours() > 4 {
			force = true
		}
	}
	var err error
	if force {
		if cc == nil {
			cc, err = MySQLConn(_this.ResourceConf)
		} else {
			err = cc.Ping()
		}
		if err != nil {
			cc, err = MySQLConn(_this.ResourceConf)
			if err != nil {
				return nil, err
			}
		}
		_this.latestConnTime = time.Now()
		_this.clientLink = cc
	}
	return cc, nil
}

// canalInstance 获取canal
func (_this *MySQLReader) canalInstance(flush bool) (*canal.Canal, error) {
	rc := _this.ResourceConf
	if _this.canalCfg == nil {
		addr := fmt.Sprintf("%s:%s", rc.Host, rc.Port)
		cfg := canal.NewDefaultConfig()
		cfg.Addr = addr
		cfg.User = rc.User
		cfg.Password = rc.Pass
		cfg.Dump.ExecutionPath = ""
		_this.canalCfg = cfg
	}
	cc := _this.canalLink
	if cc == nil {
		flush = true
	}
	if flush {
		// 需要指定表
		var tabs []string
		ds := _this.SourceConf.DocumentSet
		for docSet, _ := range ds {
			tabs = append(tabs, fmt.Sprintf("^%s$", docSet))
		}
		_this.canalCfg.IncludeTableRegex = tabs

		var err error
		cc, err = canal.NewCanal(_this.canalCfg)
		if err != nil {
			return nil, err
		}
		_this.canalLink = cc
	}
	return cc, nil
}

// Run 运行任务
func (_this *MySQLReader) Run(opC chan *msg.Op, errC chan error) {
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

	// 当为stream时需要监控退出
	switch _this.SourceConf.SyncMode {
	case cst.SyncModeReplica, cst.SyncModeStream:
		_this.exitWG.Add(1)
		x.GoSafe(func() {
			defer func() {
				_this.exitWG.Done()
			}()

			tk := time.NewTicker(1 * time.Second)
			for {
				select {
				case <-tk.C:
					if !_this.exitF {
						continue
					}
					if _this.canalLink != nil {
						_this.canalLink.Close()
						_this.canalLink = nil
					}
					return
				}
			}
		})
	}

	// 等待读取结束
	_this.exitWG.Wait()
}

// Stop 停止任务
func (_this *MySQLReader) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Err 错误信息
func (_this *MySQLReader) Err() error {
	if _this.err == EmptyError {
		return nil
	}
	return _this.err
}

// Clear 释放资源
func (_this *MySQLReader) Clear() {
	_this.Release()
	_this.errC = nil
}

// Release 释放资源
func (_this *MySQLReader) Release() {
	log.Info("taskId:" + _this.TaskId + " release")
	if _this.clientLink != nil {
		err := _this.clientLink.Close()
		if err != nil {
			_this.errC <- err
		}
		_this.clientLink = nil
	}
	if _this.dumper != nil {
		// _this.dumper
		// 暂时无法停止
		_this.dumper = nil
	}
	if _this.masterInfo != nil {
		_this.masterInfo.Stop()
		_this.masterInfo = nil
	}
}

// Dump 以dump方式导数据,依赖于源软件本身能力
func (_this *MySQLReader) Dump(opC chan *msg.Op) {
	cfg := _this.canalCfg
	dumper, err := dump.NewDumper(cfg.Dump.ExecutionPath, cfg.Addr, cfg.User, cfg.Password)
	if err != nil {
		_this.err = err
		return
	}
	_this.dumper = dumper

	for docName, _ := range _this.SourceConf.DocumentSet {
		docSet := strings.Split(docName, ",")
		dumper.AddTables(docSet[0], docSet[1])
	}

	// 读取数据,使用别名作为
	fun := func(docSet string, doc map[string]interface{}, docIdField string, docIdValue string) {
		op := msg.NewDocOp(msg.OpSourceFmtSQL, docIdValue, docIdField, _this.TaskId, doc, nil)
		op.SourceId = _this.Id
		docSetup := _this.SourceConf.DocumentSet[docSet]
		for _, aliasName := range docSetup.Alias {
			op.DocumentSet = docSet
			op.DocumentSetAlias = aliasName
			op.OptionType = msg.OptionTypeInsert
			opC <- op
		}
	}
	c, _ := canal.NewCanal(_this.canalCfg)
	h := &ext.DumpParseHandler{C: c, DataFun: fun}
	err = _this.dumper.DumpAndParse(h)
	if err != nil {
		_this.err = err
	}
}

// Direct 以查表的方式遍历数据
func (_this *MySQLReader) Direct(opC chan *msg.Op) {
	for docSet, docSetup := range _this.SourceConf.DocumentSet {
		err := _this.foreachTable(docSet, func(doc map[string]interface{}, pkField string, pkValue string) {
			for _, alias := range docSetup.Alias {
				op := msg.NewDocOp(msg.OpSourceFmtSQL, pkValue, pkField, _this.TaskId, doc, nil)
				op.SourceId = _this.Id
				op.DocumentSet = docSet
				op.DocumentSetAlias = alias
				op.OptionType = msg.OptionTypeInsert
				opC <- op
			}
		})

		if err != nil {
			_this.err = err
		}
	}
}

// Stream 流读取,在使用space的情况下,docSet可以指定多个
func (_this *MySQLReader) Stream(opC chan *msg.Op) {

	// 使用查询更新或者删除
	val, _ := _this.SourceConf.Extra["cdc_type"]
	cdcType, _ := val.(string)
	if cdcType == "field" { // canal field
		_, err := os.Stat(_this.ResumeFile)
		unixSec := int(time.Now().Unix())
		if os.IsNotExist(err) {
			data := []byte(strconv.Itoa(unixSec))
			ioutil.WriteFile(_this.ResumeFile, data, 0644)
		} else {
			bts, _ := ioutil.ReadFile(_this.ResumeFile)
			unixSec, _ = strconv.Atoi(string(bts))
		}
		err = _this.WatchFromWitchField(opC, unixSec)
		if err != nil {
			_this.err = err
		}
		return
	}

	// 使用canal读取日志
	cc, err := _this.canalInstance(true)
	if err != nil {
		_this.err = err
		return
	}

	// binlog valid
	var pos mysql.Position
	if _this.masterInfo != nil {
		pos = _this.masterInfo.GetPos()
		if !_this.ExistsBinlog(pos) {
			pos, err = cc.GetMasterPos()
			if err != nil {
				_this.err = err
			}
		}
	} else {
		pos, _ = _this.canalLink.GetMasterPos()
	}

	// watch
	err = _this.WatchFrom(cc, pos, opC)
	if err != nil {
		_this.err = err
	}
}

// ReplicaWithCanal 自定义读取/返回流
func (_this *MySQLReader) ReplicaWithCanal(opC chan *msg.Op) {

	// 0.构建canal
	cc, err := _this.canalInstance(true)

	// 1.检查 resume
	var pos mysql.Position
	binLogValid := false
	if _this.masterInfo != nil {
		pos = _this.masterInfo.GetPos()
		binLogValid = _this.ExistsBinlog(pos)
		if !binLogValid {
			pos, err = cc.GetMasterPos()
			if err != nil {
				_this.err = err
				return
			}
		}
	} else {
		pos, err = _this.canalLink.GetMasterPos()
		if err != nil {
			_this.err = err
			return
		}
	}

	// 2.binlog 无效,完整同步数据
	if !binLogValid {
		for docSet, docSetup := range _this.SourceConf.DocumentSet {
			err := _this.foreachTable(docSet, func(doc map[string]interface{}, pkField string, pkValue string) {
				for _, alias := range docSetup.Alias {
					op := msg.NewDocOp(msg.OpSourceFmtSQL, pkValue, pkField, _this.TaskId, doc, nil)
					op.SourceId = _this.Id
					op.DocumentSet = docSet
					op.DocumentSetAlias = alias
					op.OptionType = msg.OptionTypeInsert
					opC <- op
				}
			})
			if err != nil {
				_this.err = err
				return
			}
		}
	}

	// 3.启用同步
	err = _this.WatchFrom(cc, pos, opC)
	if err != nil {
		_this.err = err
	}
}

// ReplicaWithQuery 自定义读取/返回流
func (_this *MySQLReader) ReplicaWithQuery(opC chan *msg.Op) {
	_, err := os.Stat(_this.ResumeFile)
	unixSec := int(time.Now().Unix())
	hasResumeFile := true
	if os.IsNotExist(err) {
		data := []byte(strconv.Itoa(unixSec))
		ioutil.WriteFile(_this.ResumeFile, data, 0644)
		hasResumeFile = false
	} else {
		bts, _ := ioutil.ReadFile(_this.ResumeFile)
		unixSec, _ = strconv.Atoi(string(bts))
	}

	// 1.没有继续文件,读取整个表
	if !hasResumeFile {
		for docSet, docSetup := range _this.SourceConf.DocumentSet {
			err := _this.foreachTable(docSet, func(doc map[string]interface{}, pkField string, pkValue string) {
				for _, alias := range docSetup.Alias {
					op := msg.NewDocOp(msg.OpSourceFmtSQL, pkValue, pkField, _this.TaskId, doc, nil)
					op.SourceId = _this.Id
					op.DocumentSet = docSet
					op.DocumentSetAlias = alias
					op.OptionType = msg.OptionTypeInsert
					opC <- op
				}
			})
			if err != nil {
				_this.err = err
				return
			}
		}
	}

	// 2.定时更新
	err = _this.WatchFromWitchField(opC, unixSec)
	if err != nil {
		_this.err = err
	}
}

func (_this *MySQLReader) Replica(opC chan *msg.Op) {
	val, _ := _this.SourceConf.Extra["cdc_type"]
	cdcType, _ := val.(string)
	if cdcType == "field" { // canal field
		_this.ReplicaWithQuery(opC)
		return
	}
	_this.ReplicaWithCanal(opC)
}

// WatchFromWitchField 从指定时间范围查询数据
func (_this *MySQLReader) WatchFromWitchField(opC chan *msg.Op, unixSec int) (err error) {
	conn, err := MySQLConn(_this.ResourceConf)
	if err != nil {
		return err
	}
	val, _ := _this.SourceConf.Extra["cdc_field_insert"]
	insertField, _ := val.(string)
	val, _ = _this.SourceConf.Extra["cdc_field_update"]
	updateField, _ := val.(string)
	val, _ = _this.SourceConf.Extra["cdc_field_delete"]
	deleteField, _ := val.(string)
	val, _ = _this.SourceConf.Extra["cdc_field_interval"]
	fieldInterval, _ := val.(float64)

	reader := MySQLCDCFieldReader{
		conn:        conn,
		prevTime:    time.Unix(int64(unixSec), 0),
		interval:    int(fieldInterval),
		pkMap:       map[string]string{},
		tableStruct: map[string]map[string]string{},
		fields: map[string]string{
			msg.OptionTypeInsert: insertField,
			msg.OptionTypeUpdate: updateField,
			msg.OptionTypeDelete: deleteField,
		},
	}
	defer reader.Close()

	var docList []string
	docSetArr := _this.SourceConf.DocumentSet
	for name, _ := range docSetArr {
		docList = append(docList, name)
	}
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				now := reader.prevTime.Unix()
				data := []byte(strconv.Itoa(int(now)))
				ioutil.WriteFile(_this.ResumeFile, data, 0644)
				if _this.exitF {
					reader.Close()
				}
			}
		}
	}()

	// 开始从pos监听
	try := 1
	for {
		if _this.exitF {
			break
		}
		// 最大重试3次
		if try > 3 {
			_this.err = err
			break
		}
		err = reader.WatchFrom(docList, func(docSet, optionType, pkName, pkVal string, row map[string]interface{}) {
			readers, _ := _this.SourceConf.DocumentSet[docSet]
			for _, alias := range readers.Alias {
				op := msg.NewDocOp(msg.OpSourceFmtSQL, pkName, pkVal, _this.TaskId, row, nil)
				op.SourceId = _this.Id
				op.DocumentSet = docSet
				op.DocumentSetAlias = alias
				op.OptionType = optionType
				opC <- op
			}
		})
		if err != nil {
			_this.errC <- err
			time.Sleep(time.Duration(try*try) * time.Second)
			try++
			continue
		}
		try = 1
	}
	return err
}

// WatchFrom 获取pos的位置
func (_this *MySQLReader) WatchFrom(cc *canal.Canal, pos mysql.Position, opC chan *msg.Op) (err error) {

	// 使用 canal 直接读取数据
	handler := StreamEventHandler{source: _this, opC: opC}
	cc.SetEventHandler(&handler)

	// onDDL 初始化表
	if _this.onDDL {
		for docSet := range _this.SourceConf.DocumentSet {
			dbSchema := strings.Split(docSet, ".")
			dbName := dbSchema[0]
			tabName := dbSchema[1]
			dbMap, _ := _this.onDDLDocSet[dbName]
			if dbMap == nil {
				dbMap = map[string]bool{}
			}
			dbMap[tabName] = false
			_this.onDDLDocSet[dbName] = dbMap
		}
	}

	// 记录pos
	if _this.masterInfo != nil {
		_this.masterInfo.SetPos(pos)
		_this.masterInfo.savePos()
		_this.masterInfo.autoRun(cc)
	}

	// 开始从pos监听
	try := 1
	for {
		if _this.exitF {
			break
		}
		// 最大重试3次
		if try > 3 {
			_this.err = err
			break
		}
		if _this.masterInfo != nil {
			pos = _this.masterInfo.GetPos()
		}
		if cc != nil {
			err = cc.RunFrom(pos)
		}
		if err != nil {
			_this.errC <- err
			time.Sleep(time.Duration(try*try) * time.Second)
			try++
			continue
		}
		try = 1
	}
	return err
}

// SyncMode 同步模式
func (_this *MySQLReader) SyncMode() string {
	return _this.SourceConf.SyncMode
}

// RelateOneByOne 一对一关联条件
// 改方法共用连接会错误
func (_this *MySQLReader) RelateOneByOne(documentSet string, wheres []map[string][2]interface{}) (doc map[string]interface{}) {
	instance, err := _this.clientInstance(false)

	docSet := strings.Split(documentSet, ".")
	var where []string
	for _, whereItem := range wheres {
		for field, fieldItem := range whereItem {
			v := helper.SqlEscape(fmt.Sprint(fieldItem[1]))
			where = append(where, fmt.Sprintf("`%s`='%s'", field, v))
		}
	}
	whereStr := strings.Join(where, " and ")
	str := fmt.Sprintf("SELECT * FROM %s WHERE %s LIMIT 1", docSet[1], whereStr)

	// 协程错误
	_this.lock.Lock()
	instance.UseDB(docSet[0])
	ret, err := instance.Execute(str)
	_this.lock.Unlock()
	if err != nil {
		if errors.Cause(err) == mysql.ErrBadConn {
			time.Sleep(time.Second)
			instance, err = _this.clientInstance(true)
		}
		if err != nil {
			_this.errC <- fmt.Errorf("RelateOneByOne error,sql:%s message:%s", str, err)
			return nil
		}
		_this.lock.Lock()
		instance.UseDB(docSet[0])
		ret, err = instance.Execute(str)
		_this.lock.Unlock()
		if err != nil {
			_this.errC <- fmt.Errorf("RelateOneByOne error,sql:%s message:%s", str, err)
			return nil
		}
	}
	rows := MySQLReadResultToSlice(ret)
	if len(rows) > 0 {
		doc = rows[0]
	}
	return doc
}

// RelateOneByMany 一对多关联关系
func (_this *MySQLReader) RelateOneByMany(documentSet string, wheres []map[string][2]interface{}) (docs []map[string]interface{}) {
	instance, err := _this.clientInstance(false)

	docSet := strings.Split(documentSet, ".")
	var where []string
	for _, whereItem := range wheres {
		for field, fieldItem := range whereItem {
			v := helper.SqlEscape(fmt.Sprint(fieldItem[1]))
			where = append(where, fmt.Sprintf("%s='%s'", field, v))
		}
	}
	whereStr := strings.Join(where, " and ")
	str := fmt.Sprintf("SELECT * FROM %s WHERE %s ", docSet[1], whereStr)
	_this.lock.Lock()
	instance.UseDB(docSet[0])
	ret, err := instance.Execute(str)
	_this.lock.Unlock()
	if err != nil {
		if errors.Cause(err) == mysql.ErrBadConn {
			time.Sleep(time.Second)
			instance, err = _this.clientInstance(true)
		}
		if err != nil {
			_this.errC <- fmt.Errorf("RelateOneByMany error,sql:%s message:%s", str, err)
			return nil
		}
		_this.lock.Lock()
		instance.UseDB(docSet[0])
		ret, err = instance.Execute(str)
		_this.lock.Unlock()
		if err != nil {
			_this.errC <- fmt.Errorf("RelateOneByMany error,sql:%s message:%s", str, err)
			return nil
		}
	}
	docs = MySQLReadResultToSlice(ret)
	return docs
}

// RemoveResumeFile remove resume文件
func (_this *MySQLReader) RemoveResumeFile() {
	if ok, _ := helper.PathExists(_this.ResumeFile); ok {
		err := os.Remove(_this.ResumeFile)
		if err != nil {
			return
		}
	}
}

// ExistsBinlog 检查binlog,是否还在
func (_this *MySQLReader) ExistsBinlog(pos mysql.Position) bool {
	rr, err := _this.canalLink.Execute("SHOW BINARY LOGS")
	if err != nil {
		return false
	}
	idx1 := rr.Resultset.FieldNames["Log_name"]
	idx2 := rr.Resultset.FieldNames["File_size"]
	for _, value := range rr.Resultset.Values {
		vi := value[idx1].Value()
		si := value[idx2].Value()
		vStr, _ := vi.([]uint8)
		sStr, _ := si.(uint64)
		v := string(vStr)
		s := uint32(sStr)
		if v == pos.Name && s >= pos.Pos {
			return true
		}
	}
	return false
}

// Select 执行传入 sql
func (_this *MySQLReader) Select(sql string) (docs []map[string]interface{}) {
	instance, err := _this.clientInstance(false)

	if !strings.HasPrefix(strings.ToLower(sql), "select") {
		_this.errC <- fmt.Errorf("select error, sql: %s, only support select query", sql)
		return nil
	}

	result, err := instance.Execute(sql)
	if err != nil {
		_this.errC <- fmt.Errorf("select error,sql:%s message:%s", sql, err)
		return nil
	}

	return MySQLReadResultToSlice(result)
}

type rowCall func(doc map[string]interface{}, pkField string, pkValue string)

// foreachTable 便利表格
func (_this *MySQLReader) foreachTable(docSet string, call rowCall) error {
	sch := strings.Split(docSet, ".")
	if len(sch) != 2 {
		return errors.New("document set format error:" + docSet)
	}

	readTypeVal, _ := _this.SourceConf.Extra["read_type"]
	readType, _ := readTypeVal.(string)
	if readType == "stream" {
		return _this.foreachTableStream(sch[0], sch[1], call)
	}
	return _this.foreachTablePage(sch[0], sch[1], call)
}

// foreachTableQuery 遍历表数据,使用翻页的方式
func (_this *MySQLReader) foreachTablePage(db string, tab string, call rowCall) error {
	cc, err := _this.clientInstance(true)
	if err != nil {
		return err
	}

	limitValue, _ := _this.SourceConf.Extra["limit"]
	limitFloat, _ := limitValue.(float64)
	limit := int(limitFloat)
	if limit < 1 {
		limit = 100
	}

	_, pksField, err := MySQLTableStructField(cc, db, tab)
	if err != nil {
		return err
	}
	// 不支持多主键
	pkField := ""
	if len(pksField) == 1 {
		pkField = pksField[0]
	}

	// 有主键
	if pkField != "" {
		next := true
		pkVal := "0"
		for next {
			str := fmt.Sprintf("SELECT * FROM `%s` WHERE `%s`>%s ORDER BY %s ASC LIMIT %d ", tab, pkField, pkVal, pkField, limit)
			_this.lock.Lock()
			cc.UseDB(db)
			ret, err := cc.Execute(str)
			_this.lock.Unlock()
			if err != nil {
				return err
			}

			arr := MySQLReadResultToSlice(ret)
			for _, row := range arr {
				switch val := row[pkField].(type) {
				case []uint8:
					pkVal = string(val)
				case nil:
					pkVal = ""
				default:
					pkVal = fmt.Sprint(val)
				}
				call(row, pkField, pkVal)

				if _this.exitF {
					return errors.New("taskId:" + _this.TaskId + " cancel ")
				}
			}

			if _this.exitF {
				return errors.New("taskId:" + _this.TaskId + " cancel ")
			}

			if len(arr) < limit {
				next = false
			}
		}
		return nil
	}

	// 2.无主键
	// select * from tab limit 3000,1000
	offset := 0
	next := true
	for next {
		str := fmt.Sprintf("SELECT * FROM `%s` LIMIT %d,%d", tab, offset, limit)
		_this.lock.Lock()
		ret, err := cc.Execute(str)
		_this.lock.Unlock()
		if err != nil {
			return err
		}
		arr := MySQLReadResultToSlice(ret)
		for _, row := range arr {
			call(row, "", "")
			if _this.exitF {
				return errors.New("taskId:" + _this.TaskId + " cancel ")
			}
		}
		offset += limit
		if len(arr) < limit {
			next = false
		}
	}

	return nil
}

// foreachTableStream stream方式读取数据
func (_this *MySQLReader) foreachTableStream(db string, tab string, call rowCall) error {
	cc, err := _this.clientInstance(true)
	if err != nil {
		return err
	}

	// 解析表结构
	_, pksField, err := MySQLTableStructField(cc, db, tab)
	if err != nil {
		return err
	}
	var pkField string
	if len(pksField) == 1 {
		pkField = pksField[0]
	}

	// 查询
	var result mysql.Result
	strSqlVal, _ := _this.SourceConf.Extra["sql"]
	strSql, _ := strSqlVal.(string)
	if strSql == "" {
		strSql = fmt.Sprintf("SELECT * FROM `%s`.`%s`", db, tab)
	}
	return cc.ExecuteSelectStreaming(strSql, &result, func(row []mysql.FieldValue) error {
		var pkVal string
		doc := map[string]interface{}{}
		for idx, valOri := range row {

			fName := string(result.Fields[idx].Name)

			var val interface{}
			switch valOri.Type {
			case mysql.FieldValueTypeUnsigned:
				val = fmt.Sprintf("%d", valOri.AsUint64())
			case mysql.FieldValueTypeSigned:
				val = fmt.Sprintf("%d", valOri.AsInt64())
			case mysql.FieldValueTypeFloat:
				val = fmt.Sprintf("%f", valOri.AsFloat64())
				// bit数据被识别为string,转为空字符串
			case mysql.FieldValueTypeString:
				val = string(valOri.AsString())
			default: // FieldValueTypeNull
				val = nil
			}

			doc[fName] = val

			if pkField == fName {
				pkVal, _ = val.(string)
			}
		}
		call(doc, pkField, pkVal)
		return nil
	}, nil)
}

// transCanalMessage 转换消息
func (_this *MySQLReader) transCanalMessage(e *canal.RowsEvent, opC chan *msg.Op) error {

	// 记录binlog
	var binLogHeader map[string]interface{}
	if _this.blHeader {
		binLogHeader = map[string]interface{}{
			"Timestamp": e.Header.Timestamp,
			"EventType": e.Header.EventType,
			"ServerID":  e.Header.ServerID,
			"EventSize": e.Header.EventSize,
			"LogPos":    e.Header.LogPos,
			"Flags":     e.Header.Flags,
		}
	}

	documentSet := fmt.Sprintf("%s.%s", e.Table.Schema, e.Table.Name)
	readers, _ := _this.SourceConf.DocumentSet[documentSet]
	switch e.Action {
	case canal.InsertAction:
		var docId string
		docIdField := _this.getTablePKField(e)
		for _, row := range e.Rows {
			newDoc := map[string]interface{}{}
			for i, c := range e.Table.Columns {
				newDoc[c.Name] = _this.makeColumnData(&c, row[i])
				if c.Name == docIdField {
					switch a := newDoc[c.Name].(type) {
					case []uint8:
						docId = string(a)
					default:
						docId = fmt.Sprint(newDoc[c.Name])
					}
				}
			}
			if _this.blHeader {
				newDoc["__binLogHeader__"] = binLogHeader
			}

			for _, alias := range readers.Alias {
				op := msg.NewDocOp(msg.OpSourceFmtSQL, docId, docIdField, _this.TaskId, newDoc, nil)
				op.SourceId = _this.Id
				op.DocumentSet = documentSet
				op.DocumentSetAlias = alias
				op.OptionType = msg.OptionTypeInsert
				opC <- op
			}
		}
	case canal.UpdateAction:
		rl := len(e.Rows)
		if (rl % 2) == 1 {
			bytes1, _ := json.Marshal(e)
			bytes2, _ := json.Marshal(e.Rows)
			msg1 := fmt.Sprintf("event:%s\n\nRows:%s", string(bytes1), string(bytes2))
			_this.errC <- errors.New("canal.UpdateAction: len(e.Rows) != 2 " + msg1)
			return nil
		}

		var docId string
		docIdField := _this.getTablePKField(e)
		for i := 0; i < rl; i += 2 {
			oldDoc := map[string]interface{}{}
			newDoc := map[string]interface{}{}
			for i2, c := range e.Table.Columns {
				oldDoc[c.Name] = _this.makeColumnData(&c, e.Rows[i][i2])
				newDoc[c.Name] = _this.makeColumnData(&c, e.Rows[i+1][i2])
				if c.Name == docIdField {
					id, ok := oldDoc[c.Name]
					if ok {
						switch id := id.(type) {
						case []byte:
							docId = string(id)
						default:
							docId = fmt.Sprint(id)
						}
					}
				}
			}
			if _this.blHeader {
				newDoc["__binLogHeader__"] = binLogHeader
				newDoc["__oldDoc__"] = oldDoc
			}
			for _, alias := range readers.Alias {
				op := msg.NewDocOp(msg.OpSourceFmtSQL, docId, docIdField, _this.TaskId, newDoc, oldDoc)
				op.SourceId = _this.Id
				op.DocumentSet = documentSet
				op.DocumentSetAlias = alias
				op.OptionType = msg.OptionTypeUpdate
				opC <- op
			}
		}
	case canal.DeleteAction:
		var docId string
		docIdField := _this.getTablePKField(e)
		for _, row := range e.Rows {
			newDoc := map[string]interface{}{}
			for i, c := range e.Table.Columns {
				newDoc[c.Name] = _this.makeColumnData(&c, row[i])
				if c.Name == docIdField {
					id, ok := newDoc[c.Name]
					if ok {
						switch id.(type) {
						case int32:
							docId = fmt.Sprint(id)
						case string:
							docId = id.(string)
						default:
							docId = fmt.Sprint(id)
						}
					}
				}
			}
			if _this.blHeader {
				newDoc["__binLogHeader__"] = binLogHeader
			}
			for _, alias := range readers.Alias {
				op := msg.NewDocOp(msg.OpSourceFmtSQL, docId, docIdField, _this.TaskId, newDoc, newDoc)
				op.SourceId = _this.Id
				op.DocumentSet = documentSet
				op.DocumentSetAlias = alias
				op.OptionType = msg.OptionTypeDelete
				opC <- op
			}
		}
	}
	return nil
}

const mysqlDateFormat = "2006-01-02"
const mysqlDateTimeFormat = "2006-01-02 15:04:05"

// 获取主键
func (_this *MySQLReader) getTablePKField(e *canal.RowsEvent) (field string) {
	// 不考虑联合主键
	if len(e.Table.PKColumns) != 1 {
		return field
	}

	column := e.Table.GetPKColumn(0)
	return column.Name
}

// 将数据转为string
func (_this *MySQLReader) makeColumnData(col *schema.TableColumn, value interface{}) interface{} {
	switch value.(type) {
	case nil:
		return nil
	}
	switch col.Type {
	case schema.TYPE_NUMBER:
		// tinyint, smallint, int, bigint, year
		return fmt.Sprint(value)
	case schema.TYPE_MEDIUM_INT:
		return fmt.Sprint(value)
	case schema.TYPE_FLOAT:
		// float, double会丢失精度
		switch value := value.(type) {
		case float32:
			return strconv.FormatFloat(float64(value), 'f', -1, 32)
		case float64:
			return strconv.FormatFloat(value, 'f', -1, 64)
		default:
			return value
		}
	case schema.TYPE_DECIMAL:
		return fmt.Sprint(value)
	case schema.TYPE_TIME:
		return value
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				log.Warn("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				//return int64(1)
				return '1'
			}
			return '0'
			//return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case string:
			return value
		case uint8:
			return string(value)
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		switch v := value.(type) {
		case string:
			return v
		case []byte:
			return string(v)
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch v := value.(type) {
		case string:
			return v
		}
	case schema.TYPE_DATE:
		switch v := value.(type) {
		case string:
			return v
		}
	}
	return value
}

type StreamEventHandler struct {
	source *MySQLReader // 用于处理消息
	opC    chan *msg.Op // op msg.Op
}

func (h *StreamEventHandler) OnRotate(*replication.EventHeader, *replication.RotateEvent) error {
	return nil
}
func (h *StreamEventHandler) OnTableChanged(ev *replication.EventHeader, schema string, table string) error {
	if h.source.onDDL == false {
		return nil
	}
	dbMap, _ := h.source.onDDLDocSet[schema]
	_, ok := dbMap[table]
	if ok {
		dbMap[table] = true
	}
	return nil
}
func (h *StreamEventHandler) OnDDL(ev *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	if h.source.onDDL == false {
		return nil
	}
	dbName := string(queryEvent.Schema)
	tbs, ok := h.source.onDDLDocSet[dbName]
	if !ok {
		return nil
	}
	alterSQL := string(queryEvent.Query)
	stmt, err := sqlparser.ParseStrictDDL(alterSQL)
	if err != nil {
		h.source.errC <- err
		return nil
	}
	ddl, ok := stmt.(*sqlparser.DDL)
	if !ok {
		return nil
	}
	if ddl.Action != sqlparser.AlterStr {
		return nil
	}

	tbName := ddl.Table.Name.String()
	change, _ := tbs[tbName]
	if change == false {
		return nil
	}

	docSet := fmt.Sprintf("%s.%s", dbName, tbName)
	docSetup := h.source.SourceConf.DocumentSet[docSet]
	for _, aliasName := range docSetup.Alias {
		ns := strings.Split(aliasName, ".")
		mapDbName := ns[0]
		mapTbName := ns[1]
		cmd := msg.NewCmdOp(h.source.TaskId, msg.CmdTypeMySQLDDL, map[string]string{
			"dbName":    dbName,    // 源数据库名称
			"tbName":    tbName,    // 源数据表名称
			"mapDBName": mapDbName, // 映射数据库名称
			"mapTBName": mapTbName, // 映射数据表名称
			"sql":       alterSQL,
		})
		h.opC <- cmd
	}
	return nil
}
func (h *StreamEventHandler) OnXID(ev *replication.EventHeader, pos mysql.Position) error { return nil }
func (h *StreamEventHandler) OnGTID(ev *replication.EventHeader, pos mysql.GTIDSet) error { return nil }
func (h *StreamEventHandler) String() string                                              { return "StreamEventHandler" }

// OnRow 行数据
func (h *StreamEventHandler) OnRow(e *canal.RowsEvent) error {
	err := h.source.transCanalMessage(e, h.opC)
	if err != nil {
		return err
	}
	return nil
}

// OnPosSynced 保存同步位置
func (h *StreamEventHandler) OnPosSynced(ev *replication.EventHeader, pos mysql.Position, gtid mysql.GTIDSet, force bool) error {
	if h.source.masterInfo != nil {
		h.source.masterInfo.SetPos(pos)
		h.source.masterInfo.savePos()
	}
	return nil
}

type MasterInfo struct {
	pos               mysql.Position // mysql 的pos
	path              string         // 文件路径
	latestSavePosTime time.Time      // 最后保存时间
	exitF             bool           // 退出自动保存
	auto              bool           // 自动保存
}

// NewMasterInfo NewMasterInfo
func NewMasterInfo(file string) *MasterInfo {
	m := new(MasterInfo)
	m.path = file

	var p mysql.Position
	b, _ := ioutil.ReadFile(file)
	if len(b) > 0 {
		_ = json.Unmarshal(b, &p)
	} else {
		p = mysql.Position{}
	}
	m.pos = p
	return m
}

// autoRun 自动保存
func (m *MasterInfo) autoRun(cc *canal.Canal) {
	x.GoSafe(func() {
		tk := time.NewTicker(10 * time.Second)
		defer func() {
			tk.Stop()
		}()
		for {
			if cc == nil {
				return
			}
			select {
			case <-tk.C:
				m.SetPos(cc.SyncedPosition())
				m.savePos()
			}
			if m.exitF {
				return
			}
		}
	})
}

// Stop 停止 auto save
func (m *MasterInfo) Stop() {
	m.exitF = true
}

// SetPos SetPos
func (m *MasterInfo) SetPos(p mysql.Position) {
	m.pos = p
}

// GetPos GetPos
func (m *MasterInfo) GetPos() mysql.Position {
	return m.pos
}

// save 保存到文件
func (m *MasterInfo) savePos() {
	bts, err := json.Marshal(m.pos)
	if err != nil {
		return
	}
	err = ioutil.WriteFile(m.path, bts, 0644)
	if err != nil {
		return
	}
	m.latestSavePosTime = time.Now()
}

// MySQLReadResultToSlice 读取数据结果
func MySQLReadResultToSlice(result *mysql.Result) (arr []map[string]interface{}) {
	rowNumber := result.RowNumber()

	for i := 0; i < rowNumber; i++ {
		doc := map[string]interface{}{}
		for fieldName, j := range result.FieldNames {
			d, _ := result.GetValue(i, j)
			switch d.(type) {
			case nil:
				doc[fieldName] = nil
			default:
				doc[fieldName], _ = result.GetString(i, j)
			}
		}
		arr = append(arr, doc)
	}
	return arr
}

// MySQLTableStructField 获取表字段,以及主键
func MySQLTableStructField(cc *client.Conn, db string, tab string) (map[string]bool, []string, error) {
	err := cc.UseDB(db)
	if err != nil {
		return nil, nil, err
	}
	result, err := cc.Execute(fmt.Sprintf("DESC `%s`", tab))
	if err != nil {
		return nil, nil, err
	}

	var pksName []string
	ts := map[string]bool{}
	rows := MySQLReadResultToSlice(result)
	for _, row := range rows {
		fieldValue, _ := row["Field"]
		keyValue, _ := row["Key"]
		fieldName, _ := fieldValue.(string)
		fieldKey, _ := keyValue.(string)
		ts[fieldName] = false
		if fieldKey == "PRI" {
			pksName = append(pksName, fieldName)
			ts[fieldName] = true
		}
	}
	return ts, pksName, nil
}

// MySQL的CDC服务,查询字段监控数据变化
type MySQLCDCFieldReader struct {
	conn        *client.Conn      // MySQL客户端连接
	lock        sync.RWMutex      // 读写锁
	interval    int               // 间隔查询时间
	fields      map[string]string // 监控的字段
	prevTime    time.Time         // 上次使用时间
	isClose     bool              // 退出
	wg          sync.WaitGroup
	pkMap       map[string]string            // 主键
	tableStruct map[string]map[string]string // 表结构
}

func (r *MySQLCDCFieldReader) pkParse(db string, tab string) (string, error) {
	docSet := fmt.Sprintf("%s.%s", db, tab)
	pkName, ok := r.pkMap[docSet]
	if ok {
		return pkName, nil
	}
	err := r.conn.UseDB(db)
	if err != nil {
		return "", err
	}
	result, err := r.conn.Execute(fmt.Sprintf("DESC `%s`", tab))
	if err != nil {
		return "", err
	}
	rows := MySQLReadResultToSlice(result)
	stu := map[string]string{}
	for _, row := range rows {
		val, _ := row["Field"]
		fieldName, _ := val.(string)
		val, _ = row["Key"]
		fieldKey, _ := val.(string)
		if fieldKey == "PRI" {
			pkName = fieldName
			r.pkMap[docSet] = pkName
		}
		val, _ = row["Type"]
		fieldType, _ := val.(string)
		stu[fieldName] = fieldType
	}
	r.tableStruct[docSet] = stu
	if pkName == "" {
		return "", errors.New("没有找到主键")
	}
	return pkName, nil
}

func (r *MySQLCDCFieldReader) WatchFrom(docSetArr []string, call func(docSet string, op string, pkName string, pkVal string, row map[string]interface{})) error {
	if r.interval < 1 {
		r.interval = 5
	}
	for _, docSet := range docSetArr {
		arr := strings.Split(docSet, ".")
		if len(arr) != 2 {
			return errors.New("数据库和数据表格式不对, databaseName.tableName")
		}
		err := r.conn.UseDB(arr[0])
		if err != nil {
			return err
		}
	}

	selectFun := func(optionType string, docSet string, ts time.Time, pkName string) error {
		var rows []map[string]interface{}
		field, ok := r.fields[optionType]
		if field == "" {
			return nil
		}
		stu := r.tableStruct[docSet]
		fType := stu[field]
		var timeVal string
		var nextVal string
		if fType == "timestamp" {
			timeVal = ts.Format("2006-01-02 15:04:05")
			nextVal = ts.Add(time.Second * time.Duration(r.interval)).Format("2006-01-02 15:04:05")
		} else if strings.Contains(fType, "int") {
			timeVal = strconv.Itoa(int(ts.Unix()))
			nextVal = strconv.Itoa(int(ts.Unix()) + r.interval)
		} else {
			return errors.New("监控字段只支持integer和timestamp类型")
		}
		if ok && field != "" {
			sql := fmt.Sprintf("SELECT * FROM %s WHERE %s>='%s' AND %s <'%s'", docSet, field, timeVal, field, nextVal)
			result, err := r.conn.Execute(sql)
			if err != nil {
				time.Sleep(time.Second)
				result, err = r.conn.Execute(sql)
			}
			if err != nil {
				return err
			}
			rows = MySQLReadResultToSlice(result)
		}
		if len(rows) < 1 {
			return nil
		}
		for _, row := range rows {
			var pkVal string
			switch val := row[pkName].(type) {
			case []uint8:
				pkVal = string(val)
			case nil:
				pkVal = ""
			default:
				pkVal = fmt.Sprint(val)
			}
			call(docSet, optionType, pkName, pkVal, row)
		}
		return nil
	}

	r.wg.Add(1)
	t := time.NewTicker(time.Second * time.Duration(r.interval))
	defer t.Stop()
	for {
		select {
		case <-t.C:
			for _, docSet := range docSetArr {
				arr := strings.Split(docSet, ".")
				pkName, _ := r.pkParse(arr[0], arr[1])
				// 新增
				err := selectFun(msg.OptionTypeInsert, docSet, r.prevTime, pkName)
				if err != nil {
					return err
				}
				// 修改
				err = selectFun(msg.OptionTypeUpdate, docSet, r.prevTime, pkName)
				if err != nil {
					return err
				}
				// 删除
				err = selectFun(msg.OptionTypeDelete, docSet, r.prevTime, pkName)
				if err != nil {
					return err
				}
			}
			r.prevTime = r.prevTime.Add(time.Second * time.Duration(r.interval))
		}
		if r.isClose {
			break
		}
	}
	r.wg.Done()
	return nil
}

func (r *MySQLCDCFieldReader) Close() {
	r.isClose = true
	r.wg.Wait()
	if r.conn != nil {
		_ = r.conn.Close()
	}
}
