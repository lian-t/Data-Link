package linkd

// 从json创建任务

import (
	"context"
	"data-link-2.0/datalink/loop"
	"data-link-2.0/datalink/terms"
	"data-link-2.0/internal/conf"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/helper"
	"data-link-2.0/internal/jsvm"
	"data-link-2.0/internal/model"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
	"github.com/olivere/elastic/v7"
	"github.com/robertkrimen/otto"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
)

// BasicAuth 授权
func BasicAuth(h httprouter.Handle, requiredUser, requiredPassword string) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		user, password, hasAuth := r.BasicAuth()
		if hasAuth && user == requiredUser && password == requiredPassword {
			h(w, r, ps)
			return
		}
		w.Header().Set("WWW-Authenticate", "Basic realm=Restricted")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
	}
}

// Index hello
func Index(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	outputSuccess(w, "data and link !")
}

// DebugUI 首页
func DebugUI(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	if !conf.G.AppDebug {
		outputSuccess(w, "data and link !")
		return
	}
	if r.URL.Path != "/_debug_ui_" {
		outputSuccess(w, "data and link !")
		return
	}
	wd, _ := os.Getwd()
	file := fmt.Sprintf("%s/linkadmin/index.html", wd)
	_, err := helper.PathExists(file)
	if err != nil {
		outputError2(w, "error !")
		return
	}
	bts, _ := ioutil.ReadFile(file)
	fmt.Fprint(w, string(bts))
}

// playground otto虚拟机测试
func playground(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	wd, _ := os.Getwd()
	file := fmt.Sprintf("%s/linkadmin/playground.html", wd)
	_, err := helper.PathExists(file)
	if err != nil {
		outputError2(w, "错误 !")
		return
	}
	bts, _ := ioutil.ReadFile(file)
	fmt.Fprint(w, string(bts))
}

// playgroundRun otto运行虚拟机
func playgroundRun(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	input, err := formatBodyJSON(r)
	if err != nil {
		outputError2(w, err.Error())
		return
	}
	srcVal, ok := input["script"]
	src, _ := srcVal.(string)
	if !ok || src == "" {
		outputError2(w, "代码错误")
		return
	}
	vm := otto.New()
	err = jsvm.Lib(vm)
	if err = vm.Set("module", make(map[string]interface{})); err != nil {
		outputError2(w, err.Error())
		return
	}
	if _, err = vm.Run(src); err != nil {
		outputError2(w, err.Error())
		return
	}
	val, err := vm.Run("module.exports")
	if err != nil {
		outputError2(w, err.Error())
		return
	} else if !val.IsFunction() {
		outputError2(w, "module.exports 必须是一个函数")
		return
	}
	if err != nil {
		outputError2(w, err.Error())
		return
	}
	err = jsvm.LibInside(vm)
	if err != nil {
		outputError2(w, err.Error())
		return
	}
	arg := map[string]interface{}{
		"name": "name1",
		"age":  20,
	}
	_, err = vm.Call("module.exports", nil, arg, "insert", "dbname.tableName")
	if err != nil {
		outputError2(w, err.Error())
		return
	}
	outputSuccess(w, true)
}

// 将输入转为JSON
func formatBodyJSON(r *http.Request) (in map[string]interface{}, err error) {
	io := r.Body
	body, err := ioutil.ReadAll(io)
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, errors.New("empty body")
	}
	err = json.Unmarshal(body, &in)
	if err != nil {
		return nil, err
	}
	return in, nil
}

// 输出数据
func outputData(w http.ResponseWriter, code string, msg string, data interface{}) {
	v := map[string]interface{}{"code": code, "msg": msg, "data": data}
	bts, _ := json.Marshal(v)
	_, _ = fmt.Fprintln(w, string(bts))
	return
}

// 自定义错误
func outputError2(w http.ResponseWriter, msg string) {
	outputError(w, "10002", msg)
}

// code 输出数据
func outputErrorCode(w http.ResponseWriter, code string) {
	outputError(w, code, "")
}

// 输出错误
func outputError(w http.ResponseWriter, code string, msg string) {
	switch code {
	case "10500":
		msg = "inside error"
	case "10404":
		msg = "page not found"
	case "10002":
		// msg = ""
		// 自定义错误
	default:
	}
	outputData(w, code, msg, nil)
	return
}

// 输出结果
func outputSuccess(w http.ResponseWriter, data interface{}) {
	outputData(w, "000000", "success", data)
	return
}

// TaskDisplay task显示
func TaskDisplay(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	id := r.FormValue("uuid")
	if id != "" {
		t := loop.LOOP.TaskDetail(id)
		if t == nil {
			outputError2(w, "task not exists")
			return
		}
		v := t.Display()
		outputSuccess(w, v)
		return
	}
}

// TaskError 错误
func TaskError(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	taskID := r.FormValue("uuid")
	if len(taskID) != 36 {
		outputError2(w, "must input task uuid")
		return
	}
	t := loop.LOOP.TaskDetail(taskID)
	if t == nil {
		outputError2(w, "task not exists")
		return
	}
	num, _ := strconv.Atoi(r.FormValue("num"))
	if num < 1 {
		num = 100
	}
	order := r.FormValue("sort")
	if order != "asc" && order != "desc" {
		order = "desc"
	}
	page := r.FormValue("page")
	if t.IsOutError() {
		if page == "stat" {
			v := t.DisplayErrors(order, num)
			_, _ = fmt.Fprintln(w, v)
			return
		}
		wd, _ := os.Getwd()
		path := fmt.Sprintf("%s/linkadmin/stat.html", wd)
		bts, _ := ioutil.ReadFile(path)
		_, _ = fmt.Fprint(w, string(bts))
		return
	}
	v := t.DisplayErrors(order, num)
	_, _ = fmt.Fprintln(w, v)
	return
}

// TaskStateList 任务列表
func TaskStateList(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	lp := loop.LOOP
	tl := lp.TaskList()
	var vl []*loop.Task
	for _, v := range tl {
		vl = append(vl, v)
	}
	sort.SliceStable(vl, func(i, j int) bool {
		return vl[i].Ts.CreatedAt.UnixNano() > vl[j].Ts.CreatedAt.UnixNano()
	})
	var vs []interface{}
	for _, v := range vl {
		vs = append(vs, v.Display())
	}
	if vs == nil {
		vs = []interface{}{}
	}
	outputSuccess(w, vs)
	return
}

// TaskNew 新建任务
func TaskNew(w http.ResponseWriter, r *http.Request, p httprouter.Params) {

	// 解析body
	input, err := formatBodyJSON(r)
	if err != nil {
		outputError2(w, err.Error())
		return
	}

	// 简单检查文档结构
	if len(input) < 3 {
		outputError2(w, "input json format error!")
		return
	}

	// 检查task文件格式
	err = ValidTaskMap(input)
	if err != nil {
		msg := fmt.Sprintf("add task error:%s", err.Error())
		outputError2(w, msg)
		return
	}

	// 解析任务
	bts, _ := json.Marshal(input)
	tc, err := conf.New(bts)
	if err != nil {
		outputError2(w, "input json format error!")
		return
	}

	// 生产uuid
	id, _ := uuid.NewUUID()

	// 写入到文件
	filename := fmt.Sprintf("%s%s", LinkD.TaskPath, id)
	err = ioutil.WriteFile(filename, bts, 0644)
	if err != nil {
		outputError2(w, err.Error())
		return
	}

	// 添加任务
	t := loop.NewTask(tc, id.String())
	_, err = loop.LOOP.AddTask(t)
	if err != nil {
		outputError2(w, err.Error())
		return
	}

	outputSuccess(w, map[string]string{
		"taskId": id.String(),
		"notice": "add task success,run task need POST!",
	})
}

// TaskStart 启动任务
func TaskStart(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	// 解析body
	input, err := formatBodyJSON(r)
	if err != nil {
		outputError2(w, err.Error())
		return
	}
	taskIdStr, ok := input["uuid"]
	if !ok {
		outputError2(w, "need input task uuid!")
		return
	}
	taskId, ok := taskIdStr.(string)
	if !ok {
		outputError2(w, "task uuid need string!")
		return
	}
	op, ok := input["op"]
	if !ok {
		outputError2(w, "task option not support!")
		return
	}
	op, ok = op.(string)
	if !ok {
		outputError2(w, "task op need string!")
		return
	}
	switch op.(string) {
	case "start": // 启动一个任务
		_, err = loop.LOOP.RunTask(taskId)
		t := loop.LOOP.TaskDetail(taskId)
		for _, source := range t.Conf.Sources {
			if source.SyncMode == cst.SyncModeStream || source.SyncMode == cst.SyncModeReplica {
				LinkD.SetTaskAuto(map[string]interface{}{"task_auto_start": conf.TaskAutoStart{
					TaskID:  taskId,
					Running: true,
				}})
				break
			}
		}
	case "stop": // 停止一个任务
		_, err = loop.LOOP.StopTask(taskId)
		LinkD.SetTaskAuto(map[string]interface{}{"task_auto_start": conf.TaskAutoStart{
			TaskID:  taskId,
			Running: false,
		}})
	default:
		err = errors.New("task option not support")
	}
	if err != nil {
		outputError2(w, err.Error())
		return
	}

	outputSuccess(w, true)
}

// TaskReset 删除临时文件
func TaskReset(w http.ResponseWriter, r *http.Request, p httprouter.Params) {

	// 解析body
	input, err := formatBodyJSON(r)
	if err != nil {
		outputError2(w, err.Error())
		return
	}

	idv, ok := input["uuid"]
	if !ok {
		outputError2(w, "need task taskId !")
	}
	taskId, ok := idv.(string)
	if taskId == "" {
		outputError2(w, "need task taskId !")
		return
	}
	// 移除临时文件
	_, err = loop.LOOP.RemoveTaskFile(taskId)
	if err != nil {
		outputError2(w, err.Error())
		return
	}

	outputSuccess(w, "ok")
}

// TaskRebuild 删除任务,新建任务,同时清空目标数据库数据
func TaskRebuild(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	// 解析body
	input, err := formatBodyJSON(r)
	if err != nil {
		outputError2(w, err.Error())
		return
	}
	uuidStr, _ := input["uuid"]
	taskId, _ := uuidStr.(string)
	if taskId == "" {
		outputError2(w, "need input task taskId!")
		return
	}

	oldTask := loop.LOOP.TaskDetail(taskId)
	oldFile := fmt.Sprintf("%s%s", LinkD.TaskPath, taskId)
	bts, _ := ioutil.ReadFile(oldFile)

	// 1.新建任务
	newID, _ := uuid.NewUUID()
	newTask := loop.NewTask(oldTask.Conf, newID.String())
	_, err = loop.LOOP.AddTask(newTask)
	if err != nil {
		outputError2(w, err.Error())
		return
	}

	// 2.写入文件
	newFile := fmt.Sprintf("%s%s", LinkD.TaskPath, newID.String())
	err = ioutil.WriteFile(newFile, bts, 0644)
	if err != nil {
		outputError2(w, err.Error())
		return
	}

	// 3.移除旧文件
	_, err = loop.LOOP.RemoveTask(taskId)
	if err != nil {
		outputError2(w, err.Error())
		return
	}

	// 4.清空写入数据
	resDict := map[string]conf.Resource{}
	for _, resource := range oldTask.Conf.Resources {
		resDict[resource.Id] = resource
	}
	for _, writer := range oldTask.Conf.Targets {
		rc, _ := resDict[writer.ResourceId]
		err := TruncateDocumentSet(writer.DocumentSet, rc)
		if err != nil {
			outputError2(w, err.Error())
			return
		}
	}

	outputSuccess(w, true)
}

// TruncateDocumentSet 清空文档
func TruncateDocumentSet(docSets map[string]string, rc conf.Resource) error {
	switch rc.Type {
	case cst.ResourceTypeMysql:
		cc, err := terms.MySQLConn(rc)
		if err != nil {
			return err
		}
		for _, writerDoc := range docSets {
			ns := strings.Split(writerDoc, ".")
			if len(ns) != 2 {
				return errors.New("document Set format error")
			}
			_, err = cc.Execute(fmt.Sprintf("USE `%s`", ns[0]))
			if err != nil {
				return errors.New(err.Error())
			}
			_, err = cc.Execute(fmt.Sprintf("TRUNCATE TABLE `%s`", ns[1]))
			if err != nil {
				return errors.New(err.Error())
			}
		}
	case cst.ResourceTypeMongodb:
		cc, err := terms.MongoDBConn(rc)
		if err != nil {
			return err
		}
		for _, writerDoc := range docSets {
			ns := strings.Split(writerDoc, ".")
			if len(ns) != 2 {
				return errors.New("document Set format error")
			}
			coll := cc.Database(ns[0]).Collection(ns[1])
			err = coll.Drop(context.Background())
			if err != nil {
				return err
			}
			err = cc.Database(ns[0]).CreateCollection(context.Background(), ns[1])
			if err != nil {
				return err
			}
		}
	case cst.ResourceTypeElasticsearch:
		cc, err := terms.ElasticSearchConn(rc)
		if err != nil {
			return err
		}
		for _, writerDoc := range docSets {
			delQuery := elastic.NewExistsQuery("_id")
			_, err := elastic.NewDeleteByQueryService(cc).
				Index(writerDoc).
				Query(delQuery).
				Do(context.Background())
			if err != nil {
				return err
			}
		}
	case cst.ResourceTypePlaintext:
		exists, _ := helper.PathExists(rc.Dsn)
		if exists {
			err := os.Remove(rc.Dsn)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("unsupported")
	}
	return nil
}

// TaskDelete 删除文件
func TaskDelete(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	// 解析body
	input, err := formatBodyJSON(r)
	if err != nil {
		outputError2(w, err.Error())
		return
	}
	ids, ok := input["uuid"]
	if !ok {
		outputError2(w, "need input task taskId!")
		return
	}
	taskId, ok := ids.(string)
	if !ok {
		outputError2(w, "need input task taskId!")
		return
	}

	_, err = loop.LOOP.RemoveTask(taskId)
	if err != nil {
		outputError2(w, err.Error())
		return
	}

	outputSuccess(w, true)
}

// TaskDetail 任务的源文件
func TaskDetail(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	uuid := r.FormValue("uuid")
	if uuid == "" {
		fmt.Fprint(w, "debug")
		return
	}
	t := loop.LOOP.TaskDetail(uuid)
	if t == nil {
		fmt.Fprint(w, "task not exist")
		return
	}
	filepath := fmt.Sprintf("%s%s", LinkD.TaskPath, uuid)
	bts, _ := ioutil.ReadFile(filepath)
	fmt.Fprint(w, string(bts))
	return
}

// TaskClear 清空任务
func TaskClear(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	lp := loop.LOOP
	l := lp.TaskList()
	var msg string
	for _, t := range l {
		_, err := lp.StopTask(t.Uuid)
		if err != nil {
			msg += err.Error()
		}
		// 清除文件
		_, err = lp.RemoveTask(t.Uuid)
		if err != nil {
			msg += err.Error()
		}
	}
	outputSuccess(w, "/tasks/clear")
	return
}

// AppLog 获取日志
func AppLog(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	file := conf.G.LogPath
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		outputError2(w, err.Error())
		return
	}
	fmt.Fprint(w, string(bytes))
}

//  ---------- 新的一套接口,提供任务管理 ----------

// ResourcePing 资源列表
// 资源连接成功之后检查数据
func ResourcePing(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	in, _ := formatBodyJSON(r)

	m := new(model.Resource)
	m.Load(in)
	_, err := m.Ping()
	if err != nil {
		outputError2(w, err.Error())
		return
	}
	id := uuid.New()
	m.Id = id.String()
	outputSuccess(w, m.ToMap())
}

// ResourceDocumentSets 获取资源集
// 目前仅针对于MySQL有效
func ResourceDocumentSets(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	in, _ := formatBodyJSON(r)

	m := new(model.Resource)
	m.Load(in)
	if m.Id == "" {
		outputError2(w, "资源无效")
		return
	}
	sets, err := m.DocumentSets()
	if err != nil {
		outputError2(w, err.Error())
		return
	}
	outputSuccess(w, sets)
}

// TaskStateList1 运行中的任务列表
func TaskStateList1(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	in, _ := formatBodyJSON(r)
	r.ParseForm()
	fv := r.Form

	page, size := GetPagination(in, fv)
	where := map[string]interface{}{
		"page": page,
		"size": size,
	}
	v := fv.Get("desc")
	if v != "" {
		where["desc"] = v
	}
	v = fv.Get("state")
	if v != "" {
		where["state"] = v
	}

	// TODO:在内存中查询数据
	list := map[string]interface{}{}
	total := 0
	//list, total, err := m.WhereQuery(w, true)
	//if err != nil {
	//	return false, err
	//}
	data := map[string]interface{}{
		"list": list,
		"pagination": map[string]interface{}{
			"total": total,
			"page":  page,
			"size":  size,
		},
	}
	outputSuccess(w, data)
}

// GetPagination 获取分页参数
func GetPagination(in map[string]interface{}, fv url.Values) (page int64, size int64) {
	if len(in) < 1 {
		pagev := fv.Get("page")
		page, err := strconv.ParseInt(pagev, 10, 64)
		if err != nil || page < 1 {
			page = 1
		}
		sizev := fv.Get("size")
		size, err = strconv.ParseInt(sizev, 10, 64)
		if err != nil || page < 1 {
			size = 10
		}
		return page, size
	}

	v, ok := in["page"]
	if ok && v != nil {
		v1, ok := v.(float64)
		if ok && v1 > 0 {
			page = int64(v1)
		}
	}
	v, ok = in["size"]
	if ok && v != nil {
		v1, ok := v.(float64)
		if ok && v1 > 0 {
			size = int64(v1)
		}
	}
	if page < 0 {
		page = 0
	}
	if size < 1 {
		size = 10
	}
	return page, size
}

func Empty(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	fmt.Fprint(w, "Not support!\n")
}

func Logout(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	token := r.FormValue("token")
	bts, _ := LinkD.Redis.Get(token).Bytes()
	if len(bts) == 0 {
		outputError2(w, "没有登录信息")
		return
	}
	LinkD.Redis.Del(token)
	outputSuccess(w, "已经退出")
}
