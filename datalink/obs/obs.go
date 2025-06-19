package obs

import (
	"data-link-2.0/datalink/loop"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"data-link-2.0/internal/x"
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"net/http"
)

// 开启一个http服务,获取上传的日志

type Server struct {
	Type       string       // tcp|http 目前支持tcp服务
	Port       string       // 端口
	Host       string       // 端口
	httpServer *http.Server // http服务
}

// New 构建一个http服务,接受obs的消息
func New(c conf.LinkConf) *Server {
	s := new(Server)
	s.Host = c.LogObsHost
	s.Port = c.LogObsPort
	s.Type = c.LogObsType
	return s
}

// Start 启动任务
func (_this *Server) Start() {
	router := httprouter.New()
	router.GET("/", Index)
	router.POST("/log-obs/hello", Index)
	router.POST("/log-obs/send", Handle)
	addr := fmt.Sprintf("%s:%s", _this.Host, _this.Port)

	x.GoSafe(func() {
		log.Info("obs server start:" + addr)
		server := new(http.Server)
		_this.httpServer = server
		server.Addr = addr
		server.Handler = router
		err := server.ListenAndServe()
		if err != nil {
			log.Error(err.Error())
		}
	})
}

// Stop 停止任务
func (_this *Server) Stop() {
	if _this.httpServer != nil {
		err := _this.httpServer.Close()
		if err != nil {
			log.Error(err.Error())
			return
		}
	}
}

// Index hello
func Index(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	_, _ = fmt.Fprintln(w, "hello log-obs!")
}

// Handle 处理消息
func Handle(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintln(w, err.Error())
		return
	}

	// {"project1":[{}, {}], "project2":[{}, {}],}
	ms := map[string][]map[string]interface{}{}
	err = json.Unmarshal(body, &ms)
	if err != nil {
		fmt.Fprintln(w, err.Error())
		return
	}

	// 将消息路由至 Obs任务中
	d := loop.LOOP.TaskListObs()
	for project, m := range ms {
		reader, ok := d[project]
		if !ok {
			continue
		}
		mo := &msg.ObsMsg{
			Project: project,
			Data:    m,
		}
		reader.InChan <- mo
	}
	_, _ = fmt.Fprintln(w, "done!")
}
