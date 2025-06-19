package loop

import (
	"data-link-2.0/datalink/terms"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/helper"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/x"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Loop struct {
	startAt   time.Time                   // 启动时间
	tasks     map[string]*Task            // 任务列表
	obsReader map[string]*terms.ObsReader // ObsReader
	exitC     chan bool                   // 退出信号
	exitWG    sync.WaitGroup              // 退出WG
}

var LOOP *Loop

func NewLoop(c conf.LinkConf) *Loop {
	log.Info("loop Init")
	loop := new(Loop)
	loop.startAt = time.Now()
	loop.tasks = make(map[string]*Task)
	loop.exitC = make(chan bool)
	loop.obsReader = make(map[string]*terms.ObsReader)
	LOOP = loop
	return loop
}

// Start 启动服务
func (_this *Loop) Start() {
	log.Info("loop start")

	_this.exitWG.Add(1)
	x.GoSafe(func() {
		t := time.NewTimer(2 * time.Second)
		defer func() {
			t.Stop()
			_this.exitWG.Done()
		}()

		for {
			select {
			case <-_this.exitC:
				return
			case <-t.C:
				// TODO:做一些状态检查
			}
		}
	})

	// 演示,两秒后添加一条任务
	//go loop.DemoTask()
}

// Wait 等待服务结束
func (_this *Loop) Wait() {
	log.Info("loop Wait")
	_this.exitWG.Wait()
}

// Stop 任务结束,立即返回
func (_this *Loop) Stop() {
	log.Info("loop Stop")
	for _, t := range _this.TaskList() {
		if t.State == cst.TaskRun {
			t.Stop()
		}
	}
}

// AddTask 添加一个任务
func (_this *Loop) AddTask(task *Task) (ok bool, err error) {
	_, ok = _this.tasks[task.Uuid]
	if ok {
		return false, errors.New("add task duplicate:" + task.Uuid)
	}
	_this.tasks[task.Uuid] = task
	log.Info("add task,id:" + task.Uuid)
	return true, nil
}

// RunTask 启动一个任务
func (_this *Loop) RunTask(uuid string) (ok bool, err error) {
	t, ok := _this.tasks[uuid]
	if !ok {
		return false, errors.New("task not exists")
	}

	if t.State == cst.TaskRun || t.State == cst.TaskInit {
		return false, errors.New("task running")
	}

	t2 := NewTask(t.Conf, uuid)
	delete(_this.tasks, uuid)
	_this.tasks[uuid] = t2
	t2.Run()

	log.Info("run task,id:" + uuid)

	// 需要检查OBS任务
	_this.addObsTask(t2)

	return true, nil
}

// addObsTask 处理Obs任务
func (_this *Loop) addObsTask(task *Task) bool {
	// obs任务中,resource只能在source中
	isObs := false
	for _, r := range task.resM {
		if r.Type == cst.ResourceTypeObs {
			isObs = true
			break
		}
	}
	if !isObs {
		return true
	}

	// 需要通过project指向chan中,便于数据插入
	// project -> chan
	for _, imp := range task.readerM {
		o, ok := imp.(*terms.ObsReader)
		if !ok {
			continue
		}
		for project, _ := range o.ProjectSetMap {
			_this.obsReader[project] = o
		}
		break
	}
	return true
}

// StopTask 停止一个任务
func (_this *Loop) StopTask(uuid string) (ok bool, err error) {
	t, ok := _this.tasks[uuid]
	if !ok {
		return false, errors.New("task not exists")
	}
	if t.State != cst.TaskRun && t.State != cst.TaskStop {
		if t.State == cst.TaskInit {
			return false, errors.New("task init,later can be stop")
		}
		return false, errors.New("task status not running")
	}

	t.Stop()

	// 移除 Obs任务
	_this.RemoveObsTask(t)

	log.Info("stop task,id:" + uuid)
	return true, nil
}

// RemoveObsTask 处理Obs任务
func (_this *Loop) RemoveObsTask(task *Task) bool {
	// obs任务中,resource只能在source中
	isObs := false
	for _, r := range task.resM {
		if r.Type == cst.ResourceTypeObs {
			isObs = true
			break
		}
	}
	if !isObs {
		return true
	}

	// 需要通过project指向chan中,便于数据插入
	// project -> chan
	for _, imp := range task.readerM {
		o, ok := imp.(*terms.ObsReader)
		if !ok {
			continue
		}
		for project, _ := range o.ProjectSetMap {
			delete(_this.obsReader, project)
		}
		break
	}
	return true
}

// RemoveTask 删除任务时,也会删除文件
func (_this *Loop) RemoveTask(uuid string) (ok bool, err error) {
	t, ok := _this.tasks[uuid]
	if !ok {
		return true, errors.New("task not found")
	}

	// 任务是否可移除
	if t.State == cst.TaskRun || t.State == cst.TaskInit {
		return false, errors.New("task can't remove")
	}

	_, err = _this.RemoveTaskFile(uuid)
	if err != nil {
		return false, err
	}
	RemoveFlow(t.Uuid)
	delete(_this.tasks, uuid)

	log.Info("remove task,Id:" + uuid)

	return true, nil
}

// RemoveTaskFile 移除任务相关文件
func (_this *Loop) RemoveTaskFile(uuid string) (ok bool, err error) {
	t, ok := _this.tasks[uuid]
	c := conf.G

	// 删除日志文件,日志文件是切割
	err = filepath.Walk(c.TaskErrorPath, func(path string, f os.FileInfo, err error) error {
		if f == nil || f.IsDir() {
			return err
		}
		pre := filepath.Base(path)
		if strings.HasPrefix(pre, t.Uuid) {
			_ = os.Remove(path)
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	file := c.TaskErrorPath + t.Uuid + ".log"
	if ok, _ := helper.PathExists(file); ok {

		if err != nil {
			return false, err
		}
	}

	// 移除任务文件
	file = c.TaskPath + t.Uuid
	if ok, _ := helper.PathExists(file); ok {
		err := os.Remove(file)
		if err != nil {
			return false, err
		}
	}

	// 移除resume文件
	t.RemoveResumeFile()
	return true, nil
}

// TaskDetail 获取任务状态
func (_this *Loop) TaskDetail(uuid string) *Task {
	t, ok := _this.tasks[uuid]
	if ok {
		return t
	}
	return nil
}

// TaskList 任务状态
func (_this *Loop) TaskList() map[string]*Task {
	return _this.tasks
}

// TaskListObs Obs任务
func (_this *Loop) TaskListObs() map[string]*terms.ObsReader {
	return _this.obsReader
}
