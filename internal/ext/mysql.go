package ext

import (
	"encoding/hex"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	_ "github.com/xwb1989/sqlparser"
	"strconv"
	"strings"
)

type DumpParseHandler struct {
	C            *canal.Canal                                                                      // 连接信息
	name         string                                                                            // binlog名称
	pos          uint64                                                                            // 位置
	DataFun      func(ns string, doc map[string]interface{}, docIdField string, docIdValue string) // 回调参数
	tableInfoMap map[string]*schema.Table                                                          // 表信息
	tableInfoPK  map[string]string                                                                 // 表主键
}

func (h *DumpParseHandler) GtidSet(gtidsets string) error {
	return nil
}

func (h *DumpParseHandler) BinLog(name string, pos uint64) error {
	h.name = name
	h.pos = pos
	return nil
}

func (h *DumpParseHandler) Data(db string, table string, values []string) (err error) {
	ns := fmt.Sprintf("%s.%s", db, table)

	// 取表信息
	if h.tableInfoMap == nil {
		h.tableInfoMap = map[string]*schema.Table{}
		h.tableInfoPK = map[string]string{}
	}
	var tableInfo *schema.Table
	docIdField, ok := h.tableInfoPK[ns]
	tableInfo, _ = h.tableInfoMap[ns]
	if !ok {
		tableInfo, err = h.C.GetTable(db, table)
		if err != nil {
			return err
		}
		if len(tableInfo.PKColumns) == 1 && len(tableInfo.Columns) > 0 {
			docIdField = tableInfo.Columns[tableInfo.PKColumns[0]].Name
		}
		h.tableInfoMap[ns] = tableInfo
		h.tableInfoPK[ns] = docIdField
	}

	// 构建文档
	var docIdValue string
	doc := map[string]interface{}{}
	for i, v := range values {
		field := tableInfo.Columns[i].Name
		//doc[field] = v
		if field == docIdField {
			docIdValue = v
		}

		if v == "NULL" {
			doc[field] = nil
		} else if v == "_binary ''" {
			doc[field] = []byte{}
		} else if v[0] != '\'' {
			if tableInfo.Columns[i].Type == schema.TYPE_NUMBER || tableInfo.Columns[i].Type == schema.TYPE_MEDIUM_INT {
				var n interface{}
				var err error

				if tableInfo.Columns[i].IsUnsigned {
					n, err = strconv.ParseUint(v, 10, 64)
				} else {
					n, err = strconv.ParseInt(v, 10, 64)
				}
				if err != nil {
					return fmt.Errorf("parse row %v at %d error %v, int expected", values, i, err)
				}
				doc[field] = n
			} else if tableInfo.Columns[i].Type == schema.TYPE_FLOAT {
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return fmt.Errorf("parse row %v at %d error %v, float expected", values, i, err)
				}
				doc[field] = f
			} else if tableInfo.Columns[i].Type == schema.TYPE_DECIMAL {
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return fmt.Errorf("parse row %v at %d error %v, float expected", values, i, err)
				}
				doc[field] = f
			} else if strings.HasPrefix(v, "0x") {
				buf, err := hex.DecodeString(v[2:])
				if err != nil {
					return fmt.Errorf("parse row %v at %d error %v, hex literal expected", values, i, err)
				}
				doc[field] = string(buf)
			} else {
				return fmt.Errorf("parse row %v error, invalid type at %d", values, i)
			}
		} else {
			doc[field] = v[1 : len(v)-1]
		}
	}
	h.DataFun(ns, doc, docIdField, docIdValue)
	return nil
}
