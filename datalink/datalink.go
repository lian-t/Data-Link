package datalink

import (
	"data-link-2.0/datalink/linkd"
	"data-link-2.0/datalink/loop"
	"data-link-2.0/datalink/obs"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/helper"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"io/fs"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"time"
)

type DataLink struct {

	// Debug 用于输出日志
	Debug bool

	// 配置
	Conf conf.LinkConf

	// runData 运行数据
	runData conf.ServerData

	// 启动动
	startAt time.Time

	// service 容器
	Loop *loop.Loop

	// controller
	LinkD *linkd.Linkd

	// logObs
	LogObs *obs.Server

	// 退出
	exitC chan bool
}

func New(l *loop.Loop, d *linkd.Linkd, o *obs.Server, c conf.LinkConf) *DataLink {
	link := DataLink{Loop: l, LinkD: d, LogObs: o, Conf: c}
	link.exitC = make(chan bool)
	return &link
}

// Start 启动 DataLink 服务
func (d *DataLink) Start() {
	d.startAt = time.Now()
	d.Loop.Start()
	d.LinkD.Start()
	if d.LogObs != nil {
		d.LogObs.Start()
	}
	d.TaskAutoStartRun()
}

// TaskAutoStartRun 任务自启
func (d *DataLink) TaskAutoStartRun() {
	if !d.Conf.TaskAutoStart {
		return
	}
	if len(d.runData.Data) < 1 {
		d.runData = conf.ServerData{Data: map[string]conf.TaskAutoStart{}}
	}
	bytes, err := ioutil.ReadFile(d.Conf.ServicePath)
	if os.IsNotExist(err) {
		err := ioutil.WriteFile(d.Conf.ServicePath, bytes, fs.ModePerm)
		if err != nil {
			fmt.Println("task_auto_start create file error:", err.Error())
			return
		}
	} else if err != nil {
		fmt.Println("task_auto_start read file error:", err.Error())
		return
	} else if len(bytes) < 1 {
		fmt.Println("task_auto_start read file error: empty")
		return
	}
	var data conf.ServerData
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		fmt.Println("task_auto_start Unmarshal error:", err.Error())
		return
	}
	for taskID, item := range data.Data {
		if !item.Running {
			continue
		}
		_, err := d.Loop.RunTask(taskID)
		if err != nil {
			fmt.Println("task_auto_start run task error:", err.Error())
		}
		d.runData.Data[taskID] = conf.TaskAutoStart{
			TaskID:  taskID,
			Running: true,
		}
	}
}

// TaskAutoStartAdd 新增自启启动
func (d *DataLink) TaskAutoStartAdd(taskID string) {
	_, ok := d.runData.Data[taskID]
	if ok {
		return
	}
	d.runData.Data[taskID] = conf.TaskAutoStart{
		TaskID:  taskID,
		Running: true,
	}
	bytes, err := json.Marshal(d.runData)
	if err != nil {
		fmt.Println("task_auto_start run task add error:", err.Error())
		return
	}
	_ = ioutil.WriteFile(d.Conf.ServicePath, bytes, fs.ModePerm)
}

// TaskAutoStartDel 新增自启删除
func (d *DataLink) TaskAutoStartDel(taskID string) {
	_, ok := d.runData.Data[taskID]
	if !ok {
		return
	}
	delete(d.runData.Data, taskID)
	bytes, err := json.Marshal(d.runData)
	if err != nil {
		fmt.Println("task_auto_start run task add error:", err.Error())
		return
	}
	_ = ioutil.WriteFile(d.Conf.ServicePath, bytes, fs.ModePerm)
}

// Wait 等待消息或者发送通知
func (d *DataLink) Wait() {
	<-d.exitC
}

// SockServer 启动 unix 服务
func (d *DataLink) SockServer(addr string) {
	os.Remove(addr)
	l, err := net.Listen("unix", addr)
	if err != nil {
		fmt.Println("listen error:", err)
		return
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("accept error:", err)
		}

		bts := make([]byte, 1024)
		n, err := conn.Read(bts)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		// 需要注意多个byte=0的情况
		ret := d.dispatchCmd(string(bts[0:n]))
		conn.Write([]byte(ret))
		conn.Close()
	}
}

// 分发命令
func (d *DataLink) dispatchCmd(cmd string) string {
	cs := strings.Split(cmd, ":")
	if len(cs) == 0 {
		return ""
	}
	switch cs[0] {
	case "new": // 新建任务
		bts, err := ioutil.ReadFile(cs[1])
		if err != nil {
			return err.Error()
		}

		tc, err := conf.New(bts)
		if err != nil {
			return err.Error()
		}

		// 添加任务
		id, _ := uuid.NewUUID()
		t := loop.NewTask(tc, id.String())
		_, err = d.Loop.AddTask(t)
		if err != nil {
			return err.Error()
		}

		// 写入到文件中
		file := fmt.Sprintf("%s%s", d.Conf.TaskPath, id.String())
		err = ioutil.WriteFile(file, bts, 0644)
		if err != nil {
			return err.Error()
		}
		return id.String()
	case "start":
		_, err := d.Loop.RunTask(cs[1])
		if err != nil {
			return err.Error()
		}
	case "stop":
		_, err := d.Loop.StopTask(cs[1])
		if err != nil {
			return err.Error()
		}
	case "remove":
		_, err := d.Loop.RemoveTask(cs[1])
		if err != nil {
			return err.Error()
		}

		// 保存到文件
		file := d.Conf.TaskPath + cs[1]
		ok, err := helper.PathExists(file)
		if ok {
			err = os.Remove(file)
			if err != nil {
				return err.Error()
			}
		}
	case "info":
		id := strings.Trim(cs[1], string([]byte{}))
		t := d.Loop.TaskDetail(id)
		if t == nil {
			return "task not find "
		}
		v := t.Display()
		bts, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return fmt.Sprint(string(bts))
	case "list":
		l := d.Loop.TaskList()
		v := make([]interface{}, len(l))
		i := 0
		for _, t := range l {
			v[i] = t.Display()
			i++
		}
		bts, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return fmt.Sprint(string(bts))
	default:
		return ""
	}
	return "done"
}
