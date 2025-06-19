package main

import (
	"data-link-2.0/datalink"
	"data-link-2.0/datalink/linkd"
	"data-link-2.0/datalink/loop"
	"data-link-2.0/datalink/obs"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/helper"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/x"
	"flag"
	"fmt"
	"github.com/gofor-little/env"
	"net"
	_ "net/http/pprof"
	"os"
	"runtime"
)

const appDebug = true
const sockPath = "/tmp/datalink.sock"

// commandArgs 解析命令行
func commandArgs() map[string]interface{} {
	cmd := map[string]interface{}{}

	help := flag.Bool("help", false, "usage")
	f := flag.String("f", "", "配置文件路径")
	newTask := flag.String("new", "", "添加一个任务")
	start := flag.String("start", "", "启动一个任务")
	stop := flag.String("stop", "", "停止一个任务")
	remove := flag.String("remove", "", "移除一个任务")
	info := flag.String("info", "", "任务详情")
	list := flag.Bool("list", false, "任务列表")

	// 需要解析之后才能赋值
	flag.Parse()

	cmd["usage"] = *help
	cmd["confFile"] = *f
	cmd["new"] = *newTask
	cmd["start"] = *start
	cmd["stop"] = *stop
	cmd["remove"] = *remove
	cmd["info"] = *info
	cmd["list"] = *list
	return cmd
}

func New(confFile string) *datalink.DataLink {

	// 加载配置文件
	c := conf.LoadConf(confFile)
	// 加载Oa配置文件
	// 环境变量
	filePath := fmt.Sprintf("%s%c.env", c.BaseDir, os.PathSeparator)
	if err := env.Load(filePath); err != nil {
		log.Info(fmt.Sprintf("初始化环境变量失败：%s", err.Error()))
	}

	// 设置CPU运行的核数
	num1 := c.CoreNumber
	num0 := runtime.NumCPU()
	if num1 > 0 && num1 <= num0 {
		runtime.GOMAXPROCS(num1)
	}

	// 初始化日志
	log.InitLogger(c.LogPath, appDebug)
	log.Info("项目初始化")

	var o *obs.Server
	if c.LogObsEnabled {
		o = obs.New(*c)
	}
	r := loop.NewLoop(*c)
	h := linkd.Init(*c)
	server := datalink.New(r, h, o, *c)
	h.SaveData = func(data map[string]interface{}) {
		val, ok := data["task_auto_start"]
		if ok && c.TaskAutoStart {
			autoRun, yes := val.(conf.TaskAutoStart)
			if !yes {
				return
			}
			if autoRun.Running {
				server.TaskAutoStartAdd(autoRun.TaskID)
			} else {
				server.TaskAutoStartDel(autoRun.TaskID)
			}
		}
	}
	server.Debug = appDebug
	switch runtime.GOOS {
	case "darwin":
	case "windows":
	case "linux":
		x.GoSafe(func() {
			server.SockServer(sockPath)
		})
	}
	return server
}

// sendSock 发送一个信息
func sendSock(addr string, msg string) {
	conn, err := net.Dial("unix", addr)
	if err != nil {
		fmt.Fprintln(os.Stdout, err.Error())
		return
	}
	b1 := []byte(msg)
	_, err = conn.Write(b1)
	if err != nil {
		fmt.Fprintln(os.Stdout, err.Error())
		return
	}
	bts := make([]byte, 102400)
	_, err = conn.Read(bts)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println(string(bts))
	}
	conn.Close()
}

// 向服务发送命令
func sendCmd(cmd map[string]interface{}) {
	_, err := helper.PathExists(sockPath)
	if err != nil {
		panic("server sock not open:" + err.Error())
		return
	}

	for name, value := range cmd {
		var vb bool
		var vs string
		switch value.(type) {
		case string:
			vs = value.(string)
		case bool:
			vb = value.(bool)
		default:
			panic("unsupported command value ")
		}
		if !vb && vs == "" {
			continue
		}
		switch name {
		case "new":
			sendSock(sockPath, name+":"+vs)
		case "start":
			sendSock(sockPath, name+":"+vs)
		case "stop":
			sendSock(sockPath, name+":"+vs)
		case "remove":
			sendSock(sockPath, name+":"+vs)
		case "info":
			sendSock(sockPath, name+":"+vs)
		case "list":
			sendSock(sockPath, name+":"+vs)
		default:
			panic("unsupported command ")
		}
		break
	}
}

// main 检查配置
func main() {
	cmd := commandArgs()

	// 显示帮助
	usage, ok := cmd["usage"]
	if usage.(bool) || len(os.Args) == 1 {
		flag.PrintDefaults()
		return
	}

	// 运行服务,启动一个sock文件
	confFile, ok := cmd["confFile"]
	if ok && confFile != "" {
		server := New(confFile.(string))
		server.Start()
		conf.G.AppDebug = appDebug
		// 内存debug
		//http.ListenAndServe(":6060", nil)
		server.Wait()
		return
	}

	// 发送命令
	sendCmd(cmd)
}

//1.首次启动, 生成pid文件
//2.启动服务:
// 		再启动, 检查pid文件, 读取pid值, 扫描应用, 如果应用存在当前程序退出; 应用不存在就更新pid文件
//3.一般命令:
// 		检查pid文件, 发送信号
