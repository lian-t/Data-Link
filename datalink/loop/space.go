package loop

import (
	"data-link-2.0/internal/helper"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"data-link-2.0/internal/x"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BufferSpaceItem 数据项
type BufferSpaceItem struct {
	idx       int64         // id
	createAt  time.Time     // 入库时间
	expireAt  time.Time     // 过期时间
	lifetime  time.Duration // 缓存时间
	data      *msg.Op       // 数据项
	layerType string        // sib sub
	/**
	当layerType=sub时
	"layer_type_value": {
	    "sub_label": "",
	    "sub_format": "list|map"
	}
	当layerType=sib时
	"layer_type_value": {
	    "field_map": {
	        "r.id": "cate_id"
	    }
	}
	*/
	layerTypeValue map[string]interface{} // 数据组织格式
}

// BufferSpace 缓存空间,主要用于stream中的数据等待
type BufferSpace struct {
	autoIncId              int64                                 // 自增id
	MainSet                string                                // 主文档集合
	NewSet                 string                                // 文档集名称
	Lifetime               time.Duration                         // 缓存时间
	TimeoutStrategy        string                                // 超时策略:save(超时入库)|error(超时错误)
	RelateSetsWheres       []RelateSetWhere                      // 关联关系
	RelateDocumentSetsBase map[string]map[int64]*BufferSpaceItem // 数据集的数据仓库
	exitF                  bool                                  // 退出标志
	exitWG                 sync.WaitGroup                        // 退出标志
	lock                   sync.RWMutex                          // 锁
	isPause                bool                                  // 暂停
	pauseC                 chan bool                             // 用于暂停
}

// RelateSetWhere 关联集条件
type RelateSetWhere struct {
	condition      map[string]string      // 匹配条件
	layerType      string                 // 层级关系
	layerTypeValue map[string]interface{} // 层级关系值
	relateDocSet   string                 // 关联集
}

// NewBufferSpace 初始化对象
func NewBufferSpace(c map[string]interface{}) (*BufferSpace, error) {
	b := new(BufferSpace)
	b.lock = sync.RWMutex{}
	b.pauseC = make(chan bool)
	b.RelateDocumentSetsBase = map[string]map[int64]*BufferSpaceItem{}

	s, ok := c["main_set"]
	if !ok {
		return nil, errors.New("main_set not set")
	}
	b.MainSet = s.(string)

	s, ok = c["new_set"]
	if !ok {
		return nil, errors.New("new_set not set")
	}
	b.NewSet = s.(string)

	// 需要转换,使用毫秒为单位
	s, ok = c["lifetime"]
	if !ok {
		return nil, errors.New("new_set not set")
	}
	switch s := s.(type) {
	case string:
		r, err := strconv.Atoi(s)
		if err != nil {
			b.Lifetime = time.Duration(10)
		} else {
			b.Lifetime = time.Duration(r)
		}
	case float64:
		b.Lifetime = time.Duration(int64(s))
	default:
		// 默认10毫秒
		b.Lifetime = time.Duration(10)
	}
	b.Lifetime = b.Lifetime * time.Millisecond

	s, ok = c["timeout_strategy"]
	if !ok {
		return nil, errors.New("new_set not set")
	}
	b.TimeoutStrategy = s.(string)

	s1, ok := c["relate_sets_wheres"]
	if !ok {
		return nil, errors.New("relate_sets_wheres not set")
	}
	if s1 != nil {
		s2, _ := s1.([]interface{})
		if len(s2) > 0 {
			var rsw []RelateSetWhere
			for _, v2 := range s2 {
				v3, _ := v2.(map[string]interface{})
				rsw = append(rsw, NewRelateSetWhere(v3))
			}
			b.RelateSetsWheres = rsw
		}
	}

	return b, nil
}

// PauseIn 暂停In数据
func (b *BufferSpace) PauseIn() {
	b.lock.Lock()
	b.isPause = true
	b.lock.Unlock()
}

// ResumeIn 继续In数据
func (b *BufferSpace) ResumeIn() {
	b.pauseC <- true
}

// Start 启动一个缓存空间,in用于进入数据,out用于匹配数据,e用于超时数据
func (b *BufferSpace) Start(in chan *msg.Op, out chan *msg.Op, e chan *msg.Op) {
	// 数据进base
	b.exitWG.Add(1)
	x.GoSafe(func() {
		tk := time.NewTicker(2 * time.Second)
		defer func() {
			tk.Stop()
			b.exitWG.Done()
			log.Info("space in end")
		}()
		for {
			select {
			case op := <-in:
				if b.isPause {
					<-b.pauseC
					b.isPause = false
				}
				// 过滤掉cmd命令
				if op.MessageType == msg.MessageTypeCmd {
					out <- op
					continue
				}
				// 只有create和update可以处理
				if op.OptionType != msg.OptionTypeInsert && op.OptionType != msg.OptionTypeUpdate {
					continue
				}
				atomic.AddInt64(&b.autoIncId, 1)
				item := new(BufferSpaceItem)
				item.data = op
				item.createAt = time.Now()
				item.expireAt = item.createAt.Add(b.Lifetime)
				item.idx = b.autoIncId
				b.BaseAddOne(op.DocumentSet, item)
			case <-tk.C:
			}
			if b.exitF {
				return
			}
		}
	})

	// 数据出base
	b.exitWG.Add(1)
	x.GoSafe(func() {
		tk1 := time.NewTicker(b.Lifetime)
		tk2 := time.NewTicker(time.Second)
		save := b.TimeoutStrategy == "save"

		defer func() {
			tk1.Stop()
			tk2.Stop()
			b.exitWG.Done()
			log.Info("space out end")
		}()

		for {
			select {
			case <-tk1.C:
				// 超时,主表文档,附表文档会被忽略
				items := b.TimeoutItem()
				// 付文档也会被忽略
				for _, oneBase := range items {
					if len(oneBase) > 0 {
						for _, item := range oneBase {
							if save {
								out <- item.data
							} else {
								e <- item.data
							}
						}
					}
				}
			case <-tk2.C:
				// 匹配
				packs := b.MatchItem()
				if len(packs) < 1 {
					continue
				}

				// 投递,进入下一个流程
				success, errs := b.DeliverOp(packs)
				if len(success) > 0 {
					for _, op := range success {
						out <- op
					}
				}
				if len(errs) > 0 {
					for _, op := range errs {
						e <- op
					}
				}
			}
			if b.exitF {
				return
			}
		}
	})
}

// BaseAddOne 向base中添加数据
func (b *BufferSpace) BaseAddOne(setName string, i *BufferSpaceItem) {
	b.lock.Lock()
	defer b.lock.Unlock()
	m, ok := b.RelateDocumentSetsBase[setName]
	if !ok {
		m = map[int64]*BufferSpaceItem{}
	}
	m[i.idx] = i
	b.RelateDocumentSetsBase[setName] = m
}

// TimeoutItem 检查超时item
func (b *BufferSpace) TimeoutItem() map[string]map[int64]*BufferSpaceItem {
	n := time.Now()
	ret := map[string]map[int64]*BufferSpaceItem{}
	for setName, m := range b.RelateDocumentSetsBase {
		if len(m) < 1 {
			continue
		}
		setBase, ok := ret[setName]
		if !ok {
			setBase = map[int64]*BufferSpaceItem{}
		}
		b.lock.Lock()
		for itemId, item := range m {
			// 没有过期
			if item.expireAt.Before(n) {
				continue
			}
			// 过期
			setBase[itemId] = item
			d, _ := json.Marshal(item.data.Doc)
			log.Info("time out :" + string(d))

			// 从源数据中移除
			delete(m, itemId)
		}
		b.lock.Unlock()
		ret[setName] = setBase
	}
	return ret
}

// MatchItem 数据查找有效数据
func (b *BufferSpace) MatchItem() map[int64]*BufferSpaceItemPack {

	var packs map[int64]*BufferSpaceItemPack
	mainBase := b.RelateDocumentSetsBase[b.MainSet]
	for mainId, mainItem := range mainBase {

		whereCount := 0
		mainDoc := mainItem.data.Doc
		var items map[int64][]interface{}
		for _, where := range b.RelateSetsWheres {

			matchValid := false
			relateBase := b.RelateDocumentSetsBase[where.relateDocSet]
			for relateId, relateItem := range relateBase {
				relateDoc := relateItem.data.Doc
				matchCount := 0
				// 匹配单条
				for mainField, field := range where.condition {
					rvalue, ok := relateDoc[field]
					if !ok {
						break
					}
					mvalue, ok := mainDoc[mainField]
					if !ok {
						break
					}
					// NOTICE:条件匹配,更加细致的条件控制
					if fmt.Sprint(rvalue) == fmt.Sprint(mvalue) {
						matchCount++
					}
				}
				// 统计单个docSet的where
				if matchCount == len(where.condition) {
					matchValid = true
					if items == nil {
						items = map[int64][]interface{}{}
					}
					items[relateId] = []interface{}{relateItem, where}
					break
				}
			}

			// 有一个不匹配,就跳出循环
			if !matchValid {
				break
			}
			whereCount++
		}

		// 没有匹配到数据
		if whereCount != len(b.RelateSetsWheres) {
			items = nil
			continue
		}

		// 合并文档
		if packs == nil {
			packs = map[int64]*BufferSpaceItemPack{}
		}
		pack, ok := packs[mainId]
		if !ok {
			pack = new(BufferSpaceItemPack)
			pack.mainId = mainId
			pack.mainItem = mainItem
			pack.sets = map[string][]*BufferSpaceItem{}
		}
		for relateId, relateValue := range items {
			relateItem, _ := relateValue[0].(*BufferSpaceItem)
			where := relateValue[1].(RelateSetWhere)
			relateItem.layerType = where.layerType
			relateItem.layerTypeValue = where.layerTypeValue

			b.lock.Lock()
			pack.Append(relateItem)
			relateBase := b.RelateDocumentSetsBase[where.relateDocSet]
			delete(relateBase, relateId)
			b.lock.Unlock()
		}
		packs[mainId] = pack
	}

	return packs
}

// NewRelateSetWhere 关联关系
func NewRelateSetWhere(where map[string]interface{}) RelateSetWhere {
	m := RelateSetWhere{}

	setName, _ := where["set"]
	m.relateDocSet = setName.(string)

	condition, _ := where["condition"]
	conditionM, _ := condition.(map[string]interface{})
	if len(conditionM) > 0 {
		m.condition = map[string]string{}
		for k, v := range conditionM {
			value, _ := v.(string)
			m.condition[k] = value
		}
	}

	layerTypeOrigin, _ := where["layer_type"]
	m.layerType, _ = layerTypeOrigin.(string)

	layerTypeValueOrigin, _ := where["layer_type_value"]
	layerTypeValue, _ := layerTypeValueOrigin.(map[string]interface{})
	if len(layerTypeValue) > 0 {
		m.layerTypeValue = layerTypeValue
	}

	return m
}

// DeliverOp 合并数据
func (b *BufferSpace) DeliverOp(packs map[int64]*BufferSpaceItemPack) ([]*msg.Op, []*msg.Op) {
	if len(packs) < 1 {
		return nil, nil
	}

	var succ []*msg.Op
	var errs []*msg.Op
	for mainId, pack := range packs {
		b.lock.Lock()
		delete(b.RelateDocumentSetsBase[b.MainSet], mainId)
		b.lock.Unlock()

		m, err := pack.Pack(b.NewSet)
		if err != nil {
			errs = append(errs, m)
			continue
		}
		succ = append(succ, m)
	}
	return succ, errs
}

// Stop 设置标志位,用于退出
func (b *BufferSpace) Stop() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.exitF = true
	b.exitWG.Wait()
	b.RelateDocumentSetsBase = nil
	log.Info("BufferSpace stop")
}

// isRunning
func (b *BufferSpace) isRunning() bool {
	return !b.exitF
}

// BufferSpaceItemPack 组合包
type BufferSpaceItemPack struct {
	mainId    int64                         // main set中的id
	mainItem  *BufferSpaceItem              // main set中的item
	sets      map[string][]*BufferSpaceItem // 关联集中的数据
	setsOrder []string                      // 保存顺序
}

// Pack 合并数据
func (p *BufferSpaceItemPack) Pack(documentSet string) (*msg.Op, error) {

	doc := p.mainItem.data.Doc
	if len(p.sets) == 0 {
		return p.mainItem.data, nil
	}

	for _, setName := range p.setsOrder {
		items := p.sets[setName]
		for _, item := range items {
			relateOp := item.data
			relateDoc := relateOp.Doc
			switch item.layerType {
			case "sub":
				label := item.layerTypeValue["sub_label"].(string)
				format := item.layerTypeValue["sub_format"].(string)
				if format == "list" {
					arr, ok := doc[label]
					if ok {
						aaa, _ := arr.([]interface{})
						doc[label] = append(aaa, relateDoc)
					} else {
						doc[label] = []map[string]interface{}{relateDoc}
					}
				} else if format == "map" {
					doc[label] = relateDoc
				} else {
					return nil, errors.New("不支持的数据组织格式sub->format")
				}
			case "sib":
				fieldMapValue, _ := item.layerTypeValue["field_map"]
				fieldMap, _ := fieldMapValue.(map[string]interface{})
				// a.没有指定field map
				if len(fieldMap) == 0 {
					doc = helper.MapsMerge(doc, relateDoc)
					continue
				}

				// b.指定field map
				// sField主文档 rField关联文档
				sField, rField, err := splitFieldMap(fieldMap)
				if err != nil {
					return nil, err
				}
				// s为主文档,r为关联文档
				s := validEach(doc, sField)
				r := validEach(relateDoc, rField)
				doc = helper.MapsMerge(s, r)
			default:
				// 不支持的数据组织格式
				return nil, errors.New("不支持的数据组织格式layer->")
			}
		}
	}

	// 构建 msg.op
	mainOp := p.mainItem.data
	docIdValue := mainOp.DocIdValue
	docIdField := mainOp.DocIdField
	taskId := mainOp.TaskId
	op := msg.NewDocOp(mainOp.SourceFmt, docIdValue, docIdField, taskId, doc, mainOp.OriginDoc)
	op.SourceId = mainOp.SourceId
	op.DocumentSet = documentSet
	op.DocumentSetAlias = mainOp.DocumentSetAlias
	op.OptionType = mainOp.OptionType
	return op, nil
}

// 主文档
func validEach(s map[string]interface{}, fm map[string][]string) map[string]interface{} {

	// 处理有效字段
	for srcField, srcValue := range s {
		var newFieldName string
		newFieldExp, ok := fm[srcField]
		if len(newFieldExp) > 0 {
			newFieldName = newFieldExp[0]
		}
		delete(s, srcField)
		if !ok {
			continue
		}

		// 处理default值
		if srcValue == nil && len(newFieldExp) == 3 {
			srcValue = fieldMapDefaultValue(newFieldExp)
		}
		s[newFieldName] = srcValue
	}

	// 处理null值
	for _, newFieldExp := range fm {
		vField := newFieldExp[0]
		v, _ := s[vField]
		var srcValue interface{}
		if v == nil {
			if len(newFieldExp) == 3 {
				srcValue = fieldMapDefaultValue(newFieldExp)
			}
			s[vField] = srcValue
		}
	}
	return s
}

func splitFieldMap(fieldMap map[string]interface{}) (map[string][]string, map[string][]string, error) {
	fieldMapSrc := map[string][]string{}
	fieldMapRelate := map[string][]string{}
	for originField, valueField := range fieldMap {
		fa := strings.Split(originField, ".")
		vf, _ := valueField.(string)
		var vfArr []string
		vfExpArr := strings.Split(vf, "|")
		vfArr = append(vfArr, vfExpArr[0])
		if len(vfExpArr) == 2 {
			vfArr = append(vfArr, strings.Split(vfExpArr[1], ":")...)
			if len(vfArr) != 3 {
				return nil, nil, errors.New("relate field default value error:" + vf)
			}
		}
		l := len(fa)
		if l == 1 {
			fieldMapSrc[fa[0]] = vfArr
		}
		// field mapping error
		if l != 2 {
			continue
		}
		if fa[0] == "r" {
			fieldMapRelate[fa[1]] = vfArr
		} else {
			fieldMapSrc[fa[1]] = vfArr
		}
	}
	return fieldMapSrc, fieldMapRelate, nil
}

// Append 追加集合
func (p *BufferSpaceItemPack) Append(i *BufferSpaceItem) {
	set, ok := p.sets[i.data.DocumentSet]
	if !ok {
		set = []*BufferSpaceItem{}
		p.setsOrder = append(p.setsOrder, i.data.DocumentSet)
	}
	p.sets[i.data.DocumentSet] = append(set, i)
}
