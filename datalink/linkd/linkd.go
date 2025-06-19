package linkd

import (
	"data-link-2.0/datalink/loop"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/x"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/gofor-little/env"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// Linkd 提供http接口,添加任务
type Linkd struct {
	Host     string                       // ip
	Port     string                       // 端口
	TaskPath string                       // 保存数据的路径, 会拼接 "/"
	srv      http.Server                  // http服务器,预留
	exitC    chan bool                    // 退出信号
	SaveData func(map[string]interface{}) // 退出信号
	Redis    *redis.Client                // redis服务
}

var LinkD *Linkd

// Init 初始化包对象
func Init(c conf.LinkConf) *Linkd {
	d := New(c)
	LinkD = d
	return d
}

// New 一个对象
func New(c conf.LinkConf) *Linkd {
	log.Info("http server init")
	d := new(Linkd)
	d.Host = c.LinkDHost
	d.Port = c.LinkDPort
	d.TaskPath = c.TaskPath
	d.conRedis()
	return d
}

// Start start
func (d *Linkd) Start() {
	d.LoadTask()
	x.GoSafe(func() {
		d.StartHttp()
	})
}

// SetTaskAuto 保存数据
func (d *Linkd) SetTaskAuto(data map[string]interface{}) {
	if d.SaveData != nil {
		d.SaveData(data)
	}
}

// SetTaskAuto 保存数据
func (d *Linkd) conRedis() {
	db, _ := strconv.Atoi(env.Get("REDIS_DB", ""))
	ops := &redis.Options{
		Addr: fmt.Sprintf("%s:%s", env.Get("REDIS_HOST", "localhost"),
			env.Get("REDIS_PORT", "6379")),
		DB: db,
	}
	pass := env.Get("REDIS_PASS", "")
	if pass == "nil" {
		pass = ""
	}
	if pass != "" {
		ops.Password = pass
	}
	client := redis.NewClient(ops)
	_, err := client.Ping().Result()
	if err != nil {
		log.Info(fmt.Errorf("连接数据库失败：%w", err))
	}
	d.Redis = client
}

// LoadTask 加载数据
func (d *Linkd) LoadTask() {
	lp := loop.LOOP

	// 从文件夹中获取任务列表
	files, _ := ioutil.ReadDir(d.TaskPath)

	// 按照添加时间排序
	sort.SliceStable(files, func(i, j int) bool {
		return files[j].ModTime().UnixNano() > files[i].ModTime().UnixNano()
	})
	taskFileList := make([]string, 0)
	for _, f := range files {
		if !f.IsDir() {
			taskFileList = append(taskFileList, f.Name())
		}
	}
	taskFileListBody, _ := json.MarshalIndent(taskFileList, "", " ")
	log.Info(fmt.Sprintf("加载任务文件：%s", taskFileListBody))

	// 创建任务
	for _, f := range files {
		uuid := f.Name()
		if len(uuid) != 36 || f.IsDir() {
			continue
		}
		filepath := fmt.Sprintf("%s%s", d.TaskPath, uuid)
		bts, err := ioutil.ReadFile(filepath)
		if err != nil {
			log.Error(err.Error())
			continue
		}
		cc, err := conf.New(bts)
		if err != nil {
			log.Error(err.Error())
			continue
		}
		t := loop.NewTask(cc, uuid)
		_, err = lp.AddTask(t)
		if err != nil {
			log.Error(err.Error())
			continue
		}
		// 排序
		time.Sleep(time.Millisecond)
	}
}

// StartHttp 启动http服务
func (d *Linkd) StartHttp() {
	// 启动一个http服务,用于接收控制
	addr := d.Host + ":" + d.Port
	log.Info("http server start:" + addr)

	router := d.routeMap()
	err := http.ListenAndServe(addr, router)
	if err != nil {
		log.Error(err.Error())
	}
}

// 路由映射
func (d *Linkd) routeMap() *httprouter.Router {

	router := httprouter.New()
	router.GET("/", Index)
	router.GET("/playground", playground)
	router.POST("/playground_run", playgroundRun)

	// 设置了密码使用认证
	user, pass, ok := conf.G.GetBasicAuth()

	if ok { // 使用basic授权
		router.GET("/_debug_ui_", BasicAuth(DebugUI, user, pass))
		router.GET("/tasks", TaskStateList)
	} else {
		router.GET("/_debug_ui_", DebugUI)
		router.GET("/tasks", TaskStateList)
	}

	// dev 开发使用接口
	router.GET("/_detail", TaskDetail)
	router.GET("/_app_log", AppLog)
	router.DELETE("/tasks/clear", TaskClear)

	// task v1
	router.GET("/task", TaskDisplay)
	router.GET("/task_error", TaskError)
	router.PUT("/task/reset", TaskReset)
	router.PUT("/task/rebuild", TaskRebuild)
	router.PUT("/task", TaskNew)
	router.POST("/task", TaskStart)
	router.DELETE("/task", TaskDelete)

	// resources
	router.GET("/resources", Empty)
	router.GET("/resource", Empty)
	router.PUT("/resource", Empty)
	router.PUT("/resource/ping", ResourcePing)
	router.PUT("/resource/document-sets", ResourceDocumentSets)
	router.DELETE("/resource", Empty)

	// task v2
	router.GET("/v2/tasks", TaskStateList1)
	router.GET("/v2/task", Empty)

	// 资源文件
	webDir := conf.G.WebDir
	router.NotFound = http.FileServer(http.Dir(webDir))

	return router
}
