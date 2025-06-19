package terms

import (
	"context"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"data-link-2.0/internal/x"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"io/ioutil"
	"strings"
	"time"
)

type KafkaReader struct {
	ReaderBaseParams                // 继承了 Reader
	client           *kafka.Reader  // client
	latestConnTime   time.Time      // 最近使用时间
	ResumeFile       string         // 位置文件,记录偏移位置 {"offset":19191,"group_id":"datalink_group_id_9828"}
	resumeOffset     *OffsetService // 保存进度
}

// NewKafkaReader kafka 客户端
func NewKafkaReader(id int, tid string, rc conf.Resource, sc conf.Reader) *KafkaReader {

	var resumeFile string
	resume, _ := sc.Extra["resume"]
	save, _ := resume.(bool)
	if save == true {
		resumeFile = fmt.Sprintf("%s%s_%s_offset", conf.G.TaskResumePath, tid, rc.Id)
	}

	reader := new(KafkaReader)
	reader.Id = id
	reader.TaskId = tid
	reader.SourceConf = sc
	reader.ResourceConf = rc
	reader.ResumeFile = resumeFile
	reader.err = EmptyError

	// 1.加载binlog文件
	reader.resumeOffset = nil
	if reader.ResumeFile != "" {
		reader.resumeOffset = NewOffsetService(reader.ResumeFile)
	}

	return reader
}

// Run 运行任务
func (_this *KafkaReader) Run(opC chan *msg.Op, errC chan error) {
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
			// 不需要释放对象,需要处理
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
					if _this.client != nil {
						_this.client.Close()
						_this.client = nil
					}
					return
				}
			}
		})
	}

	// 等待读取结束
	_this.exitWG.Wait()
	_this.Release()
}

// Stop 停止任务
func (_this *KafkaReader) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Err 错误信息
func (_this *KafkaReader) Err() error {
	if _this.err == EmptyError {
		return nil
	}
	return _this.err
}

// Clear 清理资源
func (_this *KafkaReader) Clear() {
	_this.Release()
	_this.errC = nil
}

// Release 释放资源
func (_this *KafkaReader) Release() {
	log.Info("taskId:" + _this.TaskId + " release")
	if _this.client != nil {
		_this.client.Close()
		_this.client = nil
	}
	if _this.resumeOffset != nil {
		_this.resumeOffset.Stop()
		_this.resumeOffset = nil
	}
	_this.errC = nil
}

// SyncMode 同步模式
func (_this *KafkaReader) SyncMode() string {
	return _this.SourceConf.SyncMode
}

// RelateOneByOne 一对一关联条件
func (_this *KafkaReader) RelateOneByOne(documentSet string, wheres []map[string][2]interface{}) (doc map[string]interface{}) {
	return nil
}

// RelateOneByMany 一对多关联关系
func (_this *KafkaReader) RelateOneByMany(documentSet string, wheres []map[string][2]interface{}) (docs []map[string]interface{}) {
	return nil
}

// Stream 流读取
func (_this *KafkaReader) Stream(opC chan *msg.Op) {

	addr := _this.ResourceConf.Dsn
	var addrs []string
	if addr == "" {
		addr = fmt.Sprintf("%s:%s", _this.ResourceConf.Host, _this.ResourceConf.Port)
		addrs = append(addrs, addr)
	} else {
		addrs = strings.Split(addr, ",")
	}

	var groupID string
	var offset int64
	if _this.resumeOffset != nil {
		groupID = _this.resumeOffset.Offset.GroupId
		offset = _this.resumeOffset.Offset.Offset
	} else {
		uid, _ := uuid.NewUUID()
		groupID = fmt.Sprintf("datalink_group_id_%s", uid.String())
		offsetValue, _ := _this.SourceConf.Extra["offset"]
		offsetF, _ := offsetValue.(float64)
		if offsetF == 0 {
			offset = kafka.FirstOffset
		} else {
			offset = kafka.LastOffset
		}
	}
	dialer, err := _this.KafkaConn()
	if err != nil {
		_this.err = err
		return
	}

	// 只支持单一topic
	var topic string
	for docSet, _ := range _this.SourceConf.DocumentSet {
		topic = docSet
		break
	}

	cf := kafka.ReaderConfig{
		Brokers:        addrs,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    offset,
		Dialer:         dialer,
	}
	r := kafka.NewReader(cf)
	_this.client = r
	defer func() {
		r.Close()
	}()

	_this.WatchFrom(r, opC)
}

// Dump dump数据源
func (_this *KafkaReader) Dump(opC chan *msg.Op) {}

// Direct 常规方式查表导数据
func (_this *KafkaReader) Direct(opC chan *msg.Op) {}

// Replica 副本模式
func (_this *KafkaReader) Replica(opC chan *msg.Op) {

	addr := _this.ResourceConf.Dsn
	var addrs []string
	if addr == "" {
		addr = fmt.Sprintf("%s:%s", _this.ResourceConf.Host, _this.ResourceConf.Port)
		addrs = append(addrs, addr)
	} else {
		addrs = strings.Split(addr, ",")
	}

	var groupID string
	var offset int64
	if _this.resumeOffset != nil {
		groupID = _this.resumeOffset.Offset.GroupId
		offset = _this.resumeOffset.Offset.Offset
	} else {
		uid, _ := uuid.NewUUID()
		groupID = fmt.Sprintf("datalink_group_id_%s", uid.String())
		offset = kafka.FirstOffset // 从第一位置开始
	}
	dialer, err := _this.KafkaConn()
	if err != nil {
		_this.err = err
		return
	}

	// 只支持单一topic
	var topic string
	for docSet, _ := range _this.SourceConf.DocumentSet {
		topic = docSet
		break
	}
	cf := kafka.ReaderConfig{
		Brokers:        addrs,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    offset,
		Dialer:         dialer,
	}
	r := kafka.NewReader(cf)
	_this.client = r
	defer r.Close()

	_this.WatchFrom(r, opC)
}

func (_this *KafkaReader) WatchFrom(r *kafka.Reader, opC chan *msg.Op) {
	pkField := _this.fmtValue(_this.SourceConf.Extra, "pk_field")

	// 开启offset服务
	saveOffset := false
	if _this.resumeOffset != nil {
		saveOffset = true
		_this.resumeOffset.autoRun()
	}

	ctx := context.Background()
	for {
		if _this.exitF {
			break
		}
		m, err := r.ReadMessage(ctx)
		if err != nil {
			_this.errC <- err
			break
		}

		// 记录顺序
		if saveOffset {
			_this.resumeOffset.Offset.Offset = m.Offset
		}

		var msgBody map[string]interface{}
		err = json.Unmarshal(m.Value, &msgBody)
		if err != nil {
			_this.errC <- fmt.Errorf("kafka message deserialization error:%s \n body:%s", err.Error(), string(m.Value))
			continue
		}
		if msgBody == nil {
			_this.errC <- errors.New("mq body error:" + string(m.Value))
			continue
		}

		//标准数据格式,如果不是该结构则是为插入操作
		//{
		//	"OptionType":"insert",
		//	"data":""
		//}
		//或则
		//{}
		var doc map[string]interface{}
		ot := msg.OptionTypeInsert
		ots, ok := msgBody["OptionType"]
		if ok {
			switch ots {
			case "insert":
				ot = msg.OptionTypeInsert
			case "update":
				ot = msg.OptionTypeUpdate
			case "delete":
				ot = msg.OptionTypeDelete
			default:
				ot = msg.OptionTypeInsert
			}
			msgData, ok := msgBody["data"]
			if !ok {
				_this.errC <- errors.New("kafka message format error")
				continue
			}
			doc, _ = msgData.(map[string]interface{})
		} else {
			doc = msgBody
			ot = msg.OptionTypeInsert
		}
		if doc == nil {
			_this.errC <- errors.New("kafka body data error")
			continue
		}

		for docSet, docSetup := range _this.SourceConf.DocumentSet {
			for _, alias := range docSetup.Alias {
				pkValue := _this.fmtValue(msgBody, pkField)
				op := msg.NewDocOp(msg.OpSourceFmtNOSQL, pkValue, pkField, _this.TaskId, doc, nil)
				op.SourceId = _this.Id
				op.DocumentSet = docSet
				op.DocumentSetAlias = alias
				op.OptionType = ot
				opC <- op
			}
			// 仅支持单一topic
			break
		}
	}
}

// fmtValue 格式化值
func (_this *KafkaReader) fmtValue(m map[string]interface{}, k string) string {
	v, ok := m[k]
	if !ok {
		return ""
	}
	switch v := v.(type) {
	case string:
		return v
	case []uint8:
		return string(v)
	case nil:
		return ""
	case float64, float32:
		return fmt.Sprintf("%f", v)
	default:
		return fmt.Sprint(v)
	}
}

// KafkaConn 连接
func (_this *KafkaReader) KafkaConn() (*kafka.Dialer, error) {

	addr := _this.ResourceConf.Dsn
	var addrs []string
	if addr == "" {
		addr = fmt.Sprintf("%s:%s", _this.ResourceConf.Host, _this.ResourceConf.Port)
		addrs = append(addrs, addr)
	} else {
		addrs = strings.Split(addr, ",")
	}

	// 用户认证
	var dialer *kafka.Dialer
	user := _this.ResourceConf.User
	pass := _this.ResourceConf.Pass
	if user != "" && pass != "" {
		dialer = &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		}
		// 需要指明的认证模式
		mtValue, _ := _this.SourceConf.Extra["mechanism_type"]
		mt, _ := mtValue.(string)
		if strings.ToUpper(mt) == "scram" {
			// 默认认证模式
			mechanism, err := scram.Mechanism(scram.SHA512, user, pass)
			if err == nil {
				dialer.SASLMechanism = mechanism
			}
		} else {
			mechanism := plain.Mechanism{
				Username: user,
				Password: pass,
			}
			dialer.SASLMechanism = mechanism
		}

		_, err := dialer.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
	}

	return dialer, nil
}

// Offset 偏移量
type Offset struct {
	Offset  int64  `json:"offset"`   // offset
	GroupId string `json:"group_id"` // groupId
}

// OffsetService 保存进度
type OffsetService struct {
	Offset            Offset    // 偏移量
	path              string    // 文件路径
	latestSavePosTime time.Time // 最后保存时间
	exitF             bool      // 退出自动保存
	auto              bool      // 自动保存
}

// NewOffsetService 偏移量
func NewOffsetService(file string) *OffsetService {
	m := new(OffsetService)
	m.path = file

	var offset Offset
	bytes, err := ioutil.ReadFile(file)
	_ = json.Unmarshal(bytes, &offset)
	if err != nil || len(bytes) < 1 {
		uid, _ := uuid.NewUUID()
		groupID := fmt.Sprintf("datalink_group_id_%s", uid.String())
		m.Offset = Offset{
			Offset:  kafka.FirstOffset,
			GroupId: groupID,
		}
	} else {
		m.Offset = offset
	}

	return m
}

// autoRun 自动保存
func (m *OffsetService) autoRun() {
	x.GoSafe(func() {
		tk := time.NewTicker(2 * time.Second)
		defer func() {
			tk.Stop()
		}()
		for {
			select {
			case <-tk.C:
				offset := m.Offset.Offset
				m.savePos(offset)
			}
			if m.exitF {
				return
			}
		}
	})
}

// Stop 停止 auto save
func (m *OffsetService) Stop() {
	m.exitF = true
}

// SetPos SetPos
func (m *OffsetService) SetPos(o Offset) {
	m.Offset = o
}

// GetPos GetPos
func (m *OffsetService) GetPos() Offset {
	return m.Offset
}

// save 保存到文件
func (m *OffsetService) savePos(idx int64) {
	m.Offset.Offset = idx
	bts, err := json.Marshal(m.Offset)
	if err != nil {
		return
	}
	err = ioutil.WriteFile(m.path, bts, 0644)
	if err != nil {
		return
	}
	m.latestSavePosTime = time.Now()
}
