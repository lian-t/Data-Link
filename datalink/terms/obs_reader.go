package terms

import (
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"data-link-2.0/internal/x"
	"time"
)

// ObsReader log-obs的读取
type ObsReader struct {
	ReaderBaseParams
	ProjectSetMap map[string]chan *msg.ObsMsg // 项目
	InChan        chan *msg.ObsMsg            // 传入参数
}

// NewObsReader 构建一个 obs reader
func NewObsReader(id int, tid string, rc conf.Resource, sc conf.Reader) *ObsReader {

	reader := new(ObsReader)
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
func (_this *ObsReader) Run(opC chan *msg.Op, errC chan error) {
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
func (_this *ObsReader) Stop() {
	if _this.exitF {
		return
	}
	_this.exitF = true
	_this.exitWG.Wait()
}

// Err 错误信息
func (_this *ObsReader) Err() error {
	if _this.err == EmptyError {
		return nil
	}
	return _this.err
}

// Clear 释放资源
func (_this *ObsReader) Clear() {
	_this.Release()
	_this.errC = nil
}

// Release 释放资源
func (_this *ObsReader) Release() {
	log.Info("taskId:" + _this.TaskId + " release")
}

// Dump 以dump方式导数据,依赖于源软件本身能力
func (_this *ObsReader) Dump(opC chan *msg.Op) {
}

// Direct 以查表的方式遍历数据
func (_this *ObsReader) Direct(opC chan *msg.Op) {
}

// Stream 流读取,在使用space的情况下,docSet可以指定多个
func (_this *ObsReader) Stream(opC chan *msg.Op) {
	t := time.NewTicker(time.Second * 5)
	defer func() {
		t.Stop()
	}()
	for {
		if _this.exitF {
			return
		}
		select {
		case ms, ok := <-_this.InChan:
			if !ok {
				return
			}
			docSet := ms.Project
			for _, doc := range ms.Data {
				op := msg.NewDocOp(msg.OpSourceFmtSQL, "", "", _this.TaskId, doc, nil)
				op.SourceId = _this.Id
				docSetup := _this.SourceConf.DocumentSet[docSet]
				for _, aliasName := range docSetup.Alias {
					op.DocumentSet = docSet
					op.DocumentSetAlias = aliasName
					op.OptionType = msg.OptionTypeInsert
					opC <- op
				}
			}
		case <-t.C:
			// 检查退出
			continue
		}
	}
}

// Replica 自定义读取/返回流
func (_this *ObsReader) Replica(opC chan *msg.Op) {
}

// SyncMode 同步模式
func (_this *ObsReader) SyncMode() string {
	return _this.SourceConf.SyncMode
}

// RelateOneByOne 一对一关联条件
// 改方法共用连接会错误
func (_this *ObsReader) RelateOneByOne(documentSet string, wheres []map[string][2]interface{}) (doc map[string]interface{}) {
	return nil
}

// RelateOneByMany 一对多关联关系
func (_this *ObsReader) RelateOneByMany(documentSet string, wheres []map[string][2]interface{}) (docs []map[string]interface{}) {
	return nil
}

// RemoveResumeFile remove resume文件
func (_this *ObsReader) RemoveResumeFile() {
}
