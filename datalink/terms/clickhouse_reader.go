package terms

import (
	"context"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/msg"
	"data-link-2.0/internal/x"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/paulmach/orb"
	"github.com/shopspring/decimal"
	"math/big"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type ClickHouseReader struct {
	ReaderBaseParams                 // 继承了 Reader
	client           clickhouse.Conn // clickhouse客户端
	latestConnTime   time.Time       // 上一次连接时间
}

// NewClickHouseReader 构建es客户端
func NewClickHouseReader(id int, tid string, rc conf.Resource, sc conf.Reader) *ClickHouseReader {
	reader := new(ClickHouseReader)
	reader.Id = id
	reader.TaskId = tid
	reader.SourceConf = sc
	reader.ResourceConf = rc
	reader.err = EmptyError
	return reader
}

// clientInstance 获取实例,走的是http接口
func (_this *ClickHouseReader) clientInstance(flush bool) (clickhouse.Conn, error) {
	cc := _this.client
	if cc == nil {
		flush = true
	}
	if !flush {
		if time.Now().Sub(_this.latestConnTime).Hours() >= 4 {
			flush = true
		}
	}
	if flush {
		var err error
		if cc != nil {
			cc.Close()
		}
		cc, err = ClickHouseConn(_this.ResourceConf)
		if err != nil {
			return nil, err
		}
		_this.latestConnTime = time.Now()
		_this.client = cc
	}
	return cc, nil
}

// Run 运行任务
func (_this *ClickHouseReader) Run(opC chan *msg.Op, errC chan error) {
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
					// FIXME:应该是不支持stream
					return
				}
			}
		})
	}

	// 等待读取结束
	_this.exitWG.Wait()
}

// Stop 停止任务
func (_this *ClickHouseReader) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Err 错误信息
func (_this *ClickHouseReader) Err() error {
	if _this.err == EmptyError {
		return nil
	}
	return _this.err
}

// Clear 释放资源
func (_this *ClickHouseReader) Clear() {
	_this.Release()
	_this.errC = nil
}

// Release 释放资源
func (_this *ClickHouseReader) Release() {
	if _this.client != nil {
		_this.client.Close()
	}
	_this.client = nil
}

// SyncMode 同步模式
func (_this *ClickHouseReader) SyncMode() string {
	return _this.SourceConf.SyncMode
}

// RelateOneByOne 一对一关联条件
func (_this *ClickHouseReader) RelateOneByOne(documentSet string, wheres []map[string][2]interface{}) (doc map[string]interface{}) {
	return nil
}

// RelateOneByMany 一对多关联关系
func (_this *ClickHouseReader) RelateOneByMany(documentSet string, wheres []map[string][2]interface{}) (docs []map[string]interface{}) {
	return nil
}

// Direct 读取数据
func (_this *ClickHouseReader) Direct(opC chan *msg.Op) {
	instance, err := _this.clientInstance(true)
	if err != nil {
		_this.err = err
		return
	}

	limitValue, _ := _this.SourceConf.Extra["limit"]
	limit, _ := limitValue.(float64)
	if limit < 0 {
		limit = 100
	}

	sqlValue, _ := _this.SourceConf.Extra["sql"]
	sql, _ := sqlValue.(string)
	for docSet, docSetup := range _this.SourceConf.DocumentSet {
		query := sql
		if query == "" {
			query = fmt.Sprintf("SELECT * FROM %s ", docSet)
		}

		rows, err := instance.Query(context.Background(), query)
		if err != nil {
			_this.errC <- err
			continue
		}
		var (
			columnTypes = rows.ColumnTypes()
			vars        = make([]interface{}, len(columnTypes))
		)
		for i := range columnTypes {
			vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
		}
		for rows.Next() {
			if _this.exitF {
				return
			}
			if err := rows.Scan(vars...); err != nil {
				_this.errC <- err
				continue
			}
			doc := make(map[string]interface{}, len(columnTypes))
			for i, val := range vars {
				fieldName := columnTypes[i].Name()
				dbFieldType := columnTypes[i].DatabaseTypeName()
				doc[fieldName] = val
				switch v := val.(type) {
				case *string:
					doc[fieldName] = *v
				case *[]string:
					b1, _ := json.Marshal(v)
					doc[fieldName] = string(b1)
				case *[]uint8:
					if strings.Contains(dbFieldType, "Array(UInt8)") {
						var vs []rune
						for _, u := range *v {
							vs = append(vs, rune(u))
						}
						b1, _ := json.Marshal(vs)
						doc[fieldName] = string(b1)
					} else {
						doc[fieldName] = string(*v)
					}
				case *[]uint16:
					b1, _ := json.Marshal(v)
					doc[fieldName] = string(b1)
				case *[]uint32:
					b1, _ := json.Marshal(v)
					doc[fieldName] = string(b1)
				case *[]uint64:
					b1, _ := json.Marshal(v)
					doc[fieldName] = string(b1)
				case *[]int64:
					b1, _ := json.Marshal(v)
					doc[fieldName] = string(b1)
				case *uint8:
					doc[fieldName] = strconv.Itoa(int(*v))
				case *uint16:
					doc[fieldName] = strconv.Itoa(int(*v))
				case *uint32:
					doc[fieldName] = strconv.Itoa(int(*v))
				case *uint64:
					doc[fieldName] = strconv.FormatUint(*v, 10)
				case *float32:
					doc[fieldName] = strconv.FormatFloat(float64(*v), 'f', -1, 32)
				case *float64:
					doc[fieldName] = strconv.FormatFloat(*v, 'f', -1, 64)
				case **big.Int:
					if *v != nil {
						doc[fieldName] = (*v).String()
					} else {
						doc[fieldName] = nil
					}
				case *decimal.Decimal:
					doc[fieldName] = v.String()
				case *bool:
					doc[fieldName] = *v
				case *uuid.UUID:
					doc[fieldName] = v.String()
				case *time.Time:
					if strings.Contains(dbFieldType, "DateTime64") {
						doc[fieldName] = v.Format("2006-01-02 15:04:05.999999999")
					} else if strings.Contains(dbFieldType, "DateTime") {
						doc[fieldName] = v.Format("2006-01-02 15:04:05")
					} else if dbFieldType == "Date32" {
						doc[fieldName] = v.Format("2006-01-02")
					} else if dbFieldType == "Date" {
						doc[fieldName] = v.Format("2006-01-02")
					} else {
						doc[fieldName] = v.String()
					}
				case *net.IP:
					doc[fieldName] = v.String()
				case *orb.Point:
					b1, _ := json.Marshal(v)
					doc[fieldName] = string(b1)
				case *map[string]interface{}:
					b1, _ := json.Marshal(*v)
					doc[fieldName] = string(b1)
				case nil:
					doc[fieldName] = nil
				case *[]time.Time:
					var vs []string
					for _, t := range *v {
						if strings.Contains(dbFieldType, "DateTime64") {
							vs = append(vs, t.Format("2006-01-02 15:04:05.999999999"))
						} else if strings.Contains(dbFieldType, "DateTime") {
							vs = append(vs, t.Format("2006-01-02 15:04:05"))
						} else if strings.Contains(dbFieldType, "Date32") {
							vs = append(vs, t.Format("2006-01-02"))
						} else if strings.Contains(dbFieldType, "Date") {
							vs = append(vs, t.Format("2006-01-02"))
						} else {
							vs = append(vs, t.String())
						}
					}
					b1, _ := json.Marshal(vs)
					doc[fieldName] = string(b1)
				default:
					if strings.Contains(dbFieldType, "Nullable(") {
						n := reflect.ValueOf(v)
						for n.IsValid() && n.Type().Kind() == reflect.Ptr {
							n = n.Elem()
						}
						if n.IsValid() {
							b1, _ := json.Marshal(n.Interface())
							doc[fieldName] = string(b1)
						} else {
							doc[fieldName] = nil
						}
					} else {
						b1, _ := json.Marshal(v)
						doc[fieldName] = string(b1)
					}
				}
			}

			docIdField := "id"
			docIDVal, _ := doc[docIdField]
			docID, _ := docIDVal.(string)
			for _, alias := range docSetup.Alias {
				op := msg.NewDocOp(msg.OpSourceFmtNOSQL, docID, docIdField, _this.TaskId, doc, nil)
				op.ColumnTypes = columnTypes
				op.ColumnTypes = columnTypes
				op.SourceId = _this.Id
				op.DocumentSet = docSet
				op.DocumentSetAlias = alias
				op.OptionType = msg.OptionTypeInsert
				opC <- op
			}
		}
	}
}

// Dump dump数据源
func (_this *ClickHouseReader) Dump(opC chan *msg.Op) {}

// Stream 流读取
func (_this *ClickHouseReader) Stream(opC chan *msg.Op) {}

// Replica 副本模式
func (_this *ClickHouseReader) Replica(opC chan *msg.Op) {}
