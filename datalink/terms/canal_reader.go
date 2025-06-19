package terms

import (
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"data-link-2.0/internal/x"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// CanalReader MySQL的数据库日志的读取
type CanalReader struct {
	ReaderBaseParams
	ProjectSetMap map[string]chan *msg.ObsMsg // 项目
	InChan        chan *msg.ObsMsg            // 传入参数
}

// NewCanalReader 构建一个 obs reader
func NewCanalReader(id int, tid string, rc conf.Resource, sc conf.Reader) *CanalReader {

	reader := new(CanalReader)
	reader.Id = id
	reader.TaskId = tid
	reader.SourceConf = sc
	reader.ResourceConf = rc
	reader.err = EmptyError
	reader.InChan = make(chan *msg.ObsMsg)

	// 指定project
	d := map[string]chan *msg.ObsMsg{}
	for s, _ := range sc.DocumentSet {
		d[s] = reader.InChan
	}
	reader.ProjectSetMap = d

	return reader
}

// Run 运行任务
func (_this *CanalReader) Run(opC chan *msg.Op, errC chan error) {
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
func (_this *CanalReader) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Err 错误信息
func (_this *CanalReader) Err() error {
	if _this.err == EmptyError {
		return nil
	}
	return _this.err
}

// Clear 释放资源
func (_this *CanalReader) Clear() {
	_this.Release()
	_this.errC = nil
}

// Release 释放资源
func (_this *CanalReader) Release() {
	log.Info("taskId:" + _this.TaskId + " release")
}

// Dump 以dump方式导数据,依赖于源软件本身能力
func (_this *CanalReader) Dump(opC chan *msg.Op) {
}

// Direct 以查表的方式遍历数据
func (_this *CanalReader) Direct(opC chan *msg.Op) {
}

// Stream 流读取
func (_this *CanalReader) Stream(opC chan *msg.Op) {
	var tabs []string
	ds := _this.SourceConf.DocumentSet
	for docSet, _ := range ds {
		if docSet != "canal.stat" {
			tabs = append(tabs, fmt.Sprintf("^%s$", docSet))
		}
	}

	rc := _this.ResourceConf
	addr := fmt.Sprintf("%s:%s", rc.Host, rc.Port)
	cfg := canal.NewDefaultConfig()
	cfg.Addr = addr
	cfg.User = rc.User
	cfg.Password = rc.Pass
	cfg.Flavor = mysql.MySQLFlavor
	cfg.Dump.ExecutionPath = ""
	cfg.IncludeTableRegex = tabs
	canalClient, err := canal.NewCanal(cfg)
	if err != nil {
		_this.errC <- err
		return
	}

	handler := CanalEventHandler{source: _this, TableRowCountMap: &sync.Map{}, PrevTimestamp: time.Now(), BinLogHeader: map[string]interface{}{}}
	go func() {
		var pos mysql.Position
		startAtVal, _ := _this.SourceConf.Extra["start_at"]
		startAt, _ := startAtVal.(float64)
		if startAt < 1 {
			pos, err = canalClient.GetMasterPos()
			if err != nil {
				return
			}
		}
		canalClient.SetEventHandler(&handler)
		err = canalClient.RunFrom(pos)
		if err != nil {
			_this.errC <- err
		}
		_this.exitF = true
	}()

	val, _ := _this.SourceConf.Extra["interval"]
	interval, _ := val.(float64)
	if interval < 1 {
		interval = 1
	}
	timer := time.NewTicker(time.Millisecond * time.Duration(int64(interval)))
	for !_this.exitF {
		select {
		case <-timer.C:
			v := handler.Stat()
			// 需要约定序列数据集固定为 canal.stat
			newOp := msg.NewDocOp(msg.OpSourceFmtNOSQL, "", "", _this.TaskId, v, nil)
			newOp.DocumentSet = "canal.stat"
			newOp.DocumentSetAlias = "canal.stat"
			opC <- newOp
		}
	}
	timer.Stop()
	if canalClient != nil {
		canalClient.Close()
		canalClient = nil
	}
}

// Replica 自定义读取/返回流
func (_this *CanalReader) Replica(opC chan *msg.Op) {
}

// SyncMode 同步模式
func (_this *CanalReader) SyncMode() string {
	return _this.SourceConf.SyncMode
}

// RelateOneByOne 一对一关联条件
// 改方法共用连接会错误
func (_this *CanalReader) RelateOneByOne(string, []map[string][2]interface{}) (doc map[string]interface{}) {
	return nil
}

// RelateOneByMany 一对多关联关系
func (_this *CanalReader) RelateOneByMany(string, []map[string][2]interface{}) (docs []map[string]interface{}) {
	return nil
}

// RemoveResumeFile remove resume文件
func (_this *CanalReader) RemoveResumeFile() {
}

type CanalEventHandler struct {
	source            *CanalReader           // 用于处理消息
	RotateCount       uint32                 // 统计日志滚动
	TableChangedCount uint32                 // 修改表结构
	DDLCount          uint32                 // ddl语句
	XIDCount          uint32                 // xid
	GTIDCount         uint32                 // gtid
	PosSyncedCount    uint32                 // 日志同步
	BinLogHeader      map[string]interface{} // 日志位置
	TableRowCountMap  *sync.Map              // 表格行变化
	PrevTimestamp     time.Time              // 时间戳
	PrevEventCount    uint32                 // 事件总量
	PrevRowCount      uint32                 // 行事件总量
}

func (h *CanalEventHandler) OnRotate(ev *replication.EventHeader, rev *replication.RotateEvent) error {
	atomic.AddUint32(&h.RotateCount, 1)
	return nil
}
func (h *CanalEventHandler) OnTableChanged(ev *replication.EventHeader, schema string, table string) error {
	atomic.AddUint32(&h.TableChangedCount, 1)
	return nil
}
func (h *CanalEventHandler) OnDDL(ev *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	atomic.AddUint32(&h.DDLCount, 1)
	return nil
}
func (h *CanalEventHandler) OnXID(ev *replication.EventHeader, pos mysql.Position) error {
	atomic.AddUint32(&h.XIDCount, 1)
	return nil
}
func (h *CanalEventHandler) OnGTID(ev *replication.EventHeader, pos mysql.GTIDSet) error {
	atomic.AddUint32(&h.GTIDCount, 1)
	return nil
}
func (h *CanalEventHandler) String() string { return "CanalEventHandler" }

// OnRow 行数据
func (h *CanalEventHandler) OnRow(e *canal.RowsEvent) error {
	var count uint32
	tab := fmt.Sprintf("%s.%s_%s", e.Table.Schema, e.Table.Name, e.Action)
	switch e.Action {
	case canal.UpdateAction:
		count = uint32(len(e.Rows) / 2)
	case canal.InsertAction:
		count = uint32(len(e.Rows))
	case canal.DeleteAction:
		count = uint32(len(e.Rows))
	default:
		//pass
	}
	value, ok := h.TableRowCountMap.Load(tab)
	if ok {
		val, _ := value.(uint32)
		atomic.AddUint32(&val, count)
		h.TableRowCountMap.Store(tab, val)
	} else {
		h.TableRowCountMap.Store(tab, count)
	}
	h.BinLogHeader["Timestamp"] = e.Header.Timestamp
	h.BinLogHeader["EventType"] = e.Header.EventType
	h.BinLogHeader["ServerID"] = e.Header.ServerID
	h.BinLogHeader["EventSize"] = e.Header.EventSize
	h.BinLogHeader["LogPos"] = e.Header.LogPos
	h.BinLogHeader["Flags"] = e.Header.Flags
	return nil
}

// OnPosSynced 保存同步位置
func (h *CanalEventHandler) OnPosSynced(ev *replication.EventHeader, pos mysql.Position, gtid mysql.GTIDSet, force bool) error {
	atomic.AddUint32(&h.PosSyncedCount, 1)
	return nil
}

// Stat 数据统计
func (h *CanalEventHandler) Stat() map[string]interface{} {
	EventCount := h.RotateCount + h.TableChangedCount + h.DDLCount + h.XIDCount + h.GTIDCount + h.PosSyncedCount
	var RowCount uint32
	tabEvMap := map[string]map[string]uint32{}
	h.TableRowCountMap.Range(func(key, value interface{}) bool {
		tabKey, _ := key.(string)
		idx := strings.LastIndex(tabKey, "_")
		if idx == -1 {
			return true
		}
		tab := tabKey[:idx]
		act := tabKey[idx+1:]
		val, _ := value.(uint32)
		RowCount += val
		tabCountMap, ok := tabEvMap[tab]
		if ok {
			tabCountMap[act] = val
			tabEvMap[tab] = tabCountMap
		} else {
			tabEvMap[tab] = map[string]uint32{tab: val}
		}
		return true
	})
	EventCount += RowCount
	now := time.Now()
	mil := now.UnixMilli() - h.PrevTimestamp.UnixMilli()
	SpeedRateEvent := (EventCount - h.PrevEventCount) * 1000.0 / uint32(mil)
	SpeedRateRow := (RowCount - h.PrevRowCount) * 1000.0 / uint32(mil)
	data := map[string]interface{}{
		"RotateCount":        h.RotateCount,          // 统计日志滚动
		"TableChangedCount":  h.TableChangedCount,    // 修改表结构
		"DDLCount":           h.DDLCount,             // ddl语句
		"XIDCount":           h.XIDCount,             // xid
		"GTIDCount":          h.GTIDCount,            // gtid
		"PosSyncedCount":     h.PosSyncedCount,       // 日志同步
		"BinLogHeader":       h.BinLogHeader,         // 日志同步
		"TableRowEventCount": tabEvMap,               // 表格行变化
		"PrevTimestamp":      h.PrevTimestamp.Unix(), // 时间戳
		"PrevEventCount":     h.PrevEventCount,       // 上一次事件总量
		"PrevRowCount":       h.PrevRowCount,         // 上一次行事件总量
		"NowTimestamp":       now.Unix(),             // 时间戳
		"NowEventCount":      EventCount,             // 当前事件总量
		"NowRowCount":        RowCount,               // 当前行事件总量
		"SpeedRateEvent":     SpeedRateEvent,         // 速率
		"SpeedRateRow":       SpeedRateRow,           // 速率
	}
	h.PrevEventCount = EventCount
	h.PrevRowCount = RowCount
	h.PrevTimestamp = now
	return data
}
