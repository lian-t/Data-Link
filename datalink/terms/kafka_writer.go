package terms

import (
	"context"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/msg"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"io"
	"math/rand"
	"strconv"
	"time"
)

type KafkaWriter struct {
	WriterBaseParams                   // 继承了 Writer
	client           *kafka.Writer     // client
	extra            map[string]string // 其他参数
	latestConnTime   time.Time         // 上一次连接时间
}

// NewKafkaWriter Kafka写入
func NewKafkaWriter(id int, tid string, rc conf.Resource, tc conf.Writer) *KafkaWriter {
	writer := new(KafkaWriter)
	writer.Id = id
	writer.TaskId = tid
	writer.TargetConf = tc
	writer.DocumentSetMap = tc.DocumentSet
	writer.ResourceConf = rc
	return writer
}

// clientInstance 连接实例
func (_this *KafkaWriter) clientInstance(flush bool) (*kafka.Writer, error) {
	cc := _this.client
	if cc == nil {
		flush = true
	}
	if !flush {
		if time.Now().Sub(_this.latestConnTime).Hours() >= 4 {
			flush = true
		} else if rand.Int()%100000 == 0 {
			flush = true
		}
	}
	if flush {
		if cc != nil {
			cc.Close()
		}
		var err error
		cc, err = _this.KafkaConn()
		if err != nil {
			cc, err = _this.KafkaConn()
			if err != nil {
				return nil, err
			}
		}
		_this.latestConnTime = time.Now()
		_this.client = cc
	}
	return cc, nil
}

// Run 写入目标数据,单次写入最长时间为1分钟
func (_this *KafkaWriter) Run(ch chan []*msg.Op, errC chan error, call func(int)) {
	_this.errC = errC
	_this.exitWG.Add(1)
	timeout := time.Second * 10
	for !_this.exitF {
		ops := <-ch

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

		// 设置写入超时
		failed := 0
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		go func(ctx context.Context) {
			failed = _this.Write(ops)
			cancel()
		}(ctx)
		select {
		case <-ctx.Done():
			call(failed)
		case <-time.After(timeout):
			call(len(ops))
			cancel()
			_this.errC <- errors.New("taskId:" + _this.TaskId + ", timeout write")
			for _, op := range ops {
				bytes, _ := json.Marshal(op.Doc)
				_this.errC <- errors.New(string(bytes))
			}
		}
	}
	_this.exitWG.Done()
	_this.Release()
	_this.errC = nil
}

// Stop 停止
func (_this *KafkaWriter) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Release 清理资源
func (_this *KafkaWriter) Release() {
	if _this.client != nil {
		_this.client.Close()
	}
	_this.client = nil
	_this.DocumentSetMap = nil
	_this.extra = nil
}

// Write 向mq中写入
func (_this *KafkaWriter) Write(ops []*msg.Op) (failed int) {

	// 是否需要格式化为json格式
	f, _ := _this.TargetConf.Extra["format"]
	format, _ := f.(bool)

	// 转换并投递消息
	ctx := context.Background()
	for _, op := range ops {
		docSet, _ := _this.DocumentSetMap[op.DocumentSetAlias]
		if docSet == "" {
			_this.errC <- fmt.Errorf("kafka write documentSet empty,read:%s write:%s", op.DocumentSet, docSet)
			failed++
			continue
		}

		conn, err := _this.clientInstance(false)
		if err != nil {
			_this.errC <- err
			failed++
			continue
		}

		var msgBody map[string]interface{}
		if format {
			msgBody = map[string]interface{}{"OptionType": op.OptionType, "data": op.Doc}
		} else {
			msgBody = op.Doc
		}

		bytes, err := json.Marshal(msgBody)
		if err != nil {
			_this.errC <- fmt.Errorf("kafka writer message marshal error:%s body:%s", err.Error(), bytes)
			failed++
			continue
		}
		//{
		//	"OptionType":"insert",
		//	"data":""
		//}
		msgOne := kafka.Message{Value: bytes, Topic: docSet}
		err = conn.WriteMessages(ctx, msgOne)
		if err != nil {
			if err == io.ErrClosedPipe {
				conn, err = _this.clientInstance(true)
				if err != nil {
					_this.errC <- fmt.Errorf("kafka writer write messages error:%s body:%s", err.Error(), string(bytes))
					failed++
					continue
				}
				err = conn.WriteMessages(ctx, msgOne)
				if err != nil {
					_this.errC <- fmt.Errorf("kafka writer write messages error:%s body:%s", err.Error(), string(bytes))
					failed++
					continue
				}
			} else {
				_this.errC <- fmt.Errorf("kafka writer write messages error:%s body:%s", err.Error(), string(bytes))
				failed++
			}
		}
	}
	return failed
}

// fmtValue 格式化值
func (_this *KafkaWriter) fmtValue(m map[string]interface{}, k string) string {
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
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}

// KafkaConn 连接
func (_this *KafkaWriter) KafkaConn() (*kafka.Writer, error) {
	addr := _this.ResourceConf.Dsn
	if addr == "" {
		addr = fmt.Sprintf("%s:%s", _this.ResourceConf.Host, _this.ResourceConf.Port)
	}
	// 用户认证
	sharedTransport := &kafka.Transport{}
	user := _this.ResourceConf.User
	pass := _this.ResourceConf.Pass
	if user != "" && pass != "" {
		// 仅支持PLAIN认证模式
		sharedTransport.SASL = plain.Mechanism{
			Username: user,
			Password: pass,
		}
	}
	autoValue, _ := _this.TargetConf.Extra["auto_topic_create"]
	autoCreate, _ := autoValue.(bool)
	w := &kafka.Writer{
		Async:                  true, // 默认使用异步数据
		Addr:                   kafka.TCP(addr),
		Balancer:               &kafka.Hash{},
		Transport:              sharedTransport, // sharedTransport 提供用户认证
		MaxAttempts:            3,
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: autoCreate,
	}
	return w, nil
}
