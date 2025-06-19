package terms

import (
	"context"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/helper"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"data-link-2.0/internal/x"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"strings"
	"time"
)

type ClickHouseWriter struct {
	WriterBaseParams                 // 继承了 Writer
	client           clickhouse.Conn // clickhouse客户端
	latestConnTime   time.Time       // 上一次连接时间
}

// NewClickHouseWriter es写入
func NewClickHouseWriter(id int, tid string, rc conf.Resource, tc conf.Writer) *ClickHouseWriter {
	writer := new(ClickHouseWriter)
	writer.Id = id
	writer.TaskId = tid
	writer.TargetConf = tc
	writer.DocumentSetMap = tc.DocumentSet
	writer.ResourceConf = rc
	return writer
}

// clientInstance 获取实例,走的是http接口
func (_this *ClickHouseWriter) clientInstance(flush bool) (clickhouse.Conn, error) {
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

// Run 写入目标数据,单次写入最长时间为1分钟
func (_this *ClickHouseWriter) Run(ch chan []*msg.Op, errC chan error, call func(int)) {
	_this.errC = errC
	_this.exitWG.Add(1)

	// 默认每次写入一条
	limitValue, _ := _this.TargetConf.Extra["limit"]
	limitF, _ := limitValue.(float64)
	limit := int(limitF)
	if limit < 1 {
		limit = 1
	}

	// 默认一秒钟写入一次
	intervalValue, _ := _this.TargetConf.Extra["interval"]
	intervalF, _ := intervalValue.(float64)
	interval := int(intervalF)
	if interval < 1 {
		interval = 1
	}

	// 缓存队列,用于提高写入能力
	var needFlush bool
	var cacheList []*msg.Op // 缓存队列
	timeout := time.Second * time.Duration(interval)
	t := time.NewTicker(timeout)
	for {
		if _this.exitF {
			_this.Write(cacheList)
			t.Stop()
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
			if len(cacheList) >= 0 {
				needFlush = true
			}
		}

		if !needFlush {
			continue
		}

		// 写入数据
		failed := 0
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		x.GoSafe(func() {
			failed = _this.Write(cacheList)
			cancel()
		})
		select {
		case <-ctx.Done():
			call(failed)
		case <-time.After(timeout):
			call(len(cacheList))
			cancel()
			_this.errC <- errors.New("taskId:" + _this.TaskId + ", timeout write")
			for _, op := range cacheList {
				str, _ := json.Marshal(op.Doc)
				_this.errC <- errors.New(string(str))
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
func (_this *ClickHouseWriter) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Release 释放资源
func (_this *ClickHouseWriter) Release() {
	log.Info("clickhouse write stop")
	if _this.client != nil {
		_this.client.Close()
		_this.client = nil
	}
	_this.DocumentSetMap = nil
}

// Write 设置批量刷写
func (_this *ClickHouseWriter) Write(ops []*msg.Op) (failed int) {
	if len(ops) < 1 {
		return 0
	}

	client, err := _this.clientInstance(false)
	if err != nil {
		client, err = _this.clientInstance(true)
		if err != nil {
			_this.errC <- err
			return len(ops)
		}
	}

	opFun := func(opType string, docSet string, opList []*msg.Op) error {
		var sqlStr string
		switch opType {
		case msg.OptionTypeInsert:
			var vs, ks string
			var fieldNames []string
			for _, op := range opList {
				if len(fieldNames) < 1 {
					for fieldName := range op.Doc {
						fieldNames = append(fieldNames, fieldName)
					}
				}
				tmpKS, tmpVS := _this.buildInsertFV(op, fieldNames)
				if ks == "" {
					ks = tmpKS
				}
				vs = fmt.Sprintf("%s,(%s)", vs, tmpVS)
			}
			sqlStr = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", docSet, ks, strings.TrimPrefix(vs, ","))
			err := client.Exec(context.Background(), sqlStr)
			if err != nil {
				return fmt.Errorf("clieckhouse error:%s, %s", err.Error(), sqlStr)
			}
		case msg.OptionTypeUpdate:
			for _, op := range opList {
				set, where, err := _this.buildUpdateFV(op)
				if err != nil {
					_this.errC <- fmt.Errorf("clieckhouse error:%s %s", err.Error(), op.DocJson())
					failed++
					continue
				}
				sqlStr = fmt.Sprintf("ALTER TABLE %s UPDATE %s WHERE %s ;", docSet, set, where)
				err = client.Exec(context.Background(), sqlStr)
				if err != nil {
					_this.errC <- fmt.Errorf("clieckhouse error:%s, %s", err.Error(), sqlStr)
					failed++
					continue
				}
			}
		case msg.OptionTypeDelete:
			where := _this.buildDeleteFV(opList)
			sqlStr = fmt.Sprintf("DELETE FROM %s WHERE %s", docSet, where)
			err := client.Exec(context.Background(), sqlStr)
			if err != nil {
				return fmt.Errorf("clieckhouse error:%s, %s", err.Error(), sqlStr)
			}
		}
		return nil
	}

	// 数据分组 按docSet分组,按opType分组
	prevDocSet := ""
	prevOpType := ""
	var opList []*msg.Op
	for _, op := range ops {
		docSet, _ := _this.DocumentSetMap[op.DocumentSetAlias]
		if docSet == "" {
			_this.errC <- fmt.Errorf("DocumentSet can't find:%s", op.DocumentSetAlias)
			failed++
			continue
		}
		if prevDocSet == "" && prevOpType == "" {
			prevDocSet = docSet
			prevOpType = op.OptionType
		}
		if prevDocSet == docSet && prevOpType == op.OptionType {
			opList = append(opList, op)
			continue
		}
		err := opFun(prevOpType, prevDocSet, opList)
		if err != nil {
			_this.errC <- err
			failed += len(opList)
			opList = nil
		}
		prevDocSet = docSet
		prevOpType = op.OptionType
		opList = []*msg.Op{op}
	}
	if len(opList) > 0 {
		err := opFun(prevOpType, prevDocSet, opList)
		if err != nil {
			_this.errC <- err
			failed += len(opList)
		}
	}
	return failed
}

// buildInsertFV 新增
func (_this *ClickHouseWriter) buildInsertFV(op *msg.Op, fieldNames []string) (fieldStr string, valueStr string) {
	var ctMap map[string]driver.ColumnType
	if op.ColumnTypes != nil {
		ct, _ := op.ColumnTypes.([]driver.ColumnType)
		ctMap = make(map[string]driver.ColumnType, len(ct))
		for _, columnType := range ct {
			ctMap[columnType.Name()] = columnType
		}
	}

	if len(fieldNames) < 1 {
		for fieldName, _ := range op.Doc {
			fieldNames = append(fieldNames, fieldName)
		}
	}

	for _, fieldName := range fieldNames {
		fieldStr += fmt.Sprintf(",%s", fieldName)
		value, _ := op.Doc[fieldName]
		if len(ctMap) < 1 { // 其他数据源数据类型转换
			switch v := value.(type) {
			case nil:
				valueStr += ",NULL"
			case string:
				valueStr += fmt.Sprintf(",'%s'", helper.SqlEscape(v))
			case []uint8:
				valueStr += fmt.Sprintf(",'%s'", helper.SqlEscape(string(v)))
			case []int64:
				b1, _ := json.Marshal(v)
				valueStr += fmt.Sprintf(",'%s'", helper.SqlEscape(string(b1)))
			case []float64:
				b1, _ := json.Marshal(v)
				valueStr += fmt.Sprintf(",'%s'", helper.SqlEscape(string(b1)))
			case float64, float32:
				valueStr += fmt.Sprintf(",'%.6f'", v)
			case []interface{}, map[string]interface{}:
				b1, _ := json.Marshal(v)
				valueStr += fmt.Sprintf(",'%s'", helper.SqlEscape(string(b1)))
			default:
				valueStr += fmt.Sprintf(",'%s'", fmt.Sprint(v))
			}
		} else { // 数据来源为clickhouse
			columnType, _ := ctMap[fieldName]
			dbFieldType := columnType.DatabaseTypeName()
			if strings.HasPrefix(dbFieldType, "Array(") { // '[323123, 13, 1]'
				if strings.Contains(dbFieldType, "DateTime64") || strings.Contains(dbFieldType, "DateTime") || strings.Contains(dbFieldType, "Date32") || strings.Contains(dbFieldType, "Date") {
					valueStr += fmt.Sprintf(",%s", strings.Replace(value.(string), "\"", "'", -1))
				} else if strings.Contains(dbFieldType, "String") {
					switch value := value.(type) {
					case string:
						valueStr += fmt.Sprintf(",%s", strings.Replace(value, "\"", "'", -1))
					case []string:
						valueStr += fmt.Sprintf(",['%s']", strings.Join(value, "','"))
					default:
						valueStr += ",NULL"
					}
				} else {
					valueStr += fmt.Sprintf(",%s", strings.Replace(value.(string), "\"", "'", -1))
				}

			} else if strings.HasPrefix(dbFieldType, "Tuple(") { // {"i":10,"s":"str"} => ('str', 10)
				simple := true
				subType := dbFieldType[6:]
				for _, t := range []string{"Array", "Tuple", "Map"} {
					if strings.Contains(subType, t) {
						simple = false
						break
					}
				}
				if simple {
					var m map[string]interface{}
					_ = json.Unmarshal([]byte(value.(string)), &m)
					var vs string
					fs := strings.Split(dbFieldType[6:len(dbFieldType)-2], ",")
					for _, f := range fs {
						f1 := strings.Split(strings.TrimSpace(f), " ")
						switch v2 := m[f1[0]].(type) {
						case string:
							vs += fmt.Sprintf(",'%s'", v2)
						default:
							vs += fmt.Sprintf(",'%v'", v2)
						}
					}
					valueStr += fmt.Sprintf(",(%s)", strings.TrimPrefix(vs, ","))
				} else {
					valueStr += fmt.Sprintf(",'%s'", value.(string))
				}
			} else if strings.HasPrefix(dbFieldType, "Map(") { // {'key1':2, 'key2':20}
				switch value.(type) {
				case string:
					valueStr += fmt.Sprintf(",%s", strings.Replace(value.(string), "\"", "'", -1))
				default:
					valueStr += ",NULL"
				}
			} else if strings.HasPrefix(dbFieldType, "Point") { // '(10,10)'
				switch value.(type) {
				case string:
					value := []byte(value.(string))
					valueStr += fmt.Sprintf(",(%s)", value[1:len(value)-1])
				default:
					valueStr += ",NULL"
				}
			} else {
				valueStr += fmt.Sprintf(",'%s'", fmt.Sprint(value))
			}
		}
	}
	fieldStr = strings.TrimLeft(fieldStr, ",")
	valueStr = strings.TrimLeft(valueStr, ",")
	return fieldStr, valueStr
}

// buildUpdateFV 凭借修改KV值和where
func (_this *ClickHouseWriter) buildUpdateFV(op *msg.Op) (set string, where string, err error) {
	var ctMap map[string]driver.ColumnType
	if op.ColumnTypes != nil {
		ct, _ := op.ColumnTypes.([]driver.ColumnType)
		ctMap = make(map[string]driver.ColumnType, len(ct))
		for _, columnType := range ct {
			ctMap[columnType.Name()] = columnType
		}
	}
	// 拼接 where
	where = fmt.Sprintf("`%s`='%s' ", op.DocIdField, op.DocIdValue)

	// 拼接 set
	if op.SourceFmt == msg.OpSourceFmtSQL {
		for field, value := range op.Doc {
			if field == op.DocIdField {
				continue
			}
			switch v := value.(type) {
			case []uint8:
				set += fmt.Sprintf(",`%s`='%s' ", field, helper.SqlEscape(string(v)))
			case string:
				set += fmt.Sprintf(",`%s`='%s' ", field, helper.SqlEscape(v))
			case nil:
				set += fmt.Sprintf(",`%s`=NULL ", field)
			case uint8, int8, int, int32, int64, uint, uint32, uint64:
				set += fmt.Sprintf(",`%s`='%d' ", field, v)
			case float64, float32:
				set += fmt.Sprintf(",`%s`='%.6f' ", field, v)
			case []interface{}, map[string]interface{}:
				bts, _ := json.Marshal(v)
				set += fmt.Sprintf(",`%s`='%s' ", field, helper.SqlEscape(string(bts)))
			default:
				set += fmt.Sprintf(",`%s`='%s' ", field, fmt.Sprint(v))
			}
		}
		set = strings.TrimLeft(set, ",")
		return set, where, nil
	}

	// 需要处理 NOSQL 写入 SQL,NOSQl文档必有ID
	for field, value := range op.Doc {
		if field == op.DocIdField {
			continue
		}
		switch v := value.(type) {
		case []uint8:
			set += fmt.Sprintf(",`%s`='%s' ", field, helper.SqlEscape(string(v)))
		case string:
			set += fmt.Sprintf(",`%s`='%s' ", field, helper.SqlEscape(v))
		case nil:
			set += fmt.Sprintf(",`%s`=NULL ", field)
		case uint8, int8, int, int32, int64, uint, uint32, uint64:
			set += fmt.Sprintf(",`%s`='%d' ", field, v)
		case float64, float32:
			set += fmt.Sprintf(",`%s`='%.6f' ", field, v)
		default:
			set += fmt.Sprintf(",`%s`='%s' ", field, fmt.Sprint(v))
		}
	}
	set = strings.TrimLeft(set, ",")
	where = strings.TrimLeft(where, ",")
	return set, where, nil
}

// buildDeleteFV 凭借修改KV值和where
func (_this *ClickHouseWriter) buildDeleteFV(ops []*msg.Op) (where string) {
	if len(ops) == 1 {
		op := ops[0]
		where = fmt.Sprintf("%s='%s'", op.DocIdField, op.DocIdValue)
	} else {
		var docIdField string
		var ids []string
		for _, op := range ops {
			if docIdField == "" {
				docIdField = op.DocIdField
			}
			ids = append(ids, fmt.Sprintf("'%s'", op.DocIdValue))
		}

		where = fmt.Sprintf("%s in (%s)", docIdField, strings.Join(ids, ","))
	}

	return
}

func (_this *ClickHouseWriter) bodyAsString(ops []*msg.Op) string {
	var a []map[string]string
	for _, op := range ops {
		bs, _ := json.Marshal(op)
		a = append(a, map[string]string{
			op.OptionType: string(bs),
		})
	}
	str, _ := json.Marshal(a)
	return string(str)
}
