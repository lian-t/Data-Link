package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/fsnotify/fsnotify"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

var help = flag.Bool("help", false, "the help.")
var processFileName = "obs.prs"

func main() {
	flag.Parse()

	// 显示帮助
	if *help {
		flag.PrintDefaults()
		return
	}

	// 配置文件
	cf := os.Args[len(os.Args)-1]
	c, err := config(cf)
	if err != nil {
		log.Fatal(err.Error())
	}

	// 运行任务
	run(c)
}

// run 运行任务
func run(c Config) {
	// 0 退出信号
	e := make(chan bool)

	// 1 注册信号
	s := make(chan os.Signal, 1)
	signal.Notify(s, os.Interrupt, os.Kill)

	// 2.0 发送数据
	p := NewPostLink(c.Server.Host, c.Server.Protocol)
	if !p.HttpPing() {
		log.Println("post link failed")
		return
	}
	p.Run()
	defer p.Stop()

	// 2.1 监控文件
	w, err := NewWatcher(c)
	if err != nil {
		log.Println(err.Error())
		return
	}
	// 2.1.1 读取文件,对比文件和缓存文件
	c.Items = DiffData(c)
	for name, i := range c.Items {
		// 2.1.1.1 读取文件
		fp, err := MateFile(i, func(b []byte) {
			p.AppendItem(NewMsg(name, b))
		})
		if err != nil {
			log.Printf("mate file:%s error:%s", name, err.Error())
			continue
		}

		err = w.Add(fp, i)
		if err != nil {
			log.Printf("add watcher:%s error:%s", name, err.Error())
			continue
		}
		w.SaveData(sData)
	}

	// 2.2 监控数据
	log.Println("log obs running ... ")
	t := time.NewTicker(time.Second * 10)
	go func() {
		defer func() {
			w.SaveData(sData)
			t.Stop()
			e <- true
		}()
		for {
			select {
			case b, ok := <-w.Out:
				if !ok {
					return
				}
				p.AppendItem(b)
			case e, ok := <-w.Err:
				if !ok {
					return
				}
				log.Println(e.Error())
			case <-s:
				log.Println("done, Bye-bye !")
				return
			case <-t.C:
				w.SaveData(sData)
				w.CloseItemIfNeed()
			}
		}
	}()

	// 监控消息
	w.Run()
	// 等待退出
	<-e
	w.Stop()
}

// MateFile 打开文件,并且读取元数据
func MateFile(i *Item, call func([]byte)) (fp string, err error) {
	if i.IsDir {
		// 考虑空目录的情况
		if i.PathFile == "" {
			fp = i.PathDir
			return
		}
		err = ReadFile(i.PathFile, i.SeekPos, call)
	} else {
		err = ReadFile(i.PathFile, i.SeekPos, call)
	}

	fp = i.PathFile
	if err != nil {
		fp = ""
		err = fmt.Errorf("open file:%s error:%s", i.PathFile, err.Error())
	}
	return
}

// ReadFile 打开文件,并且读取到文件最后
// 读取已经有的数据只能按行读取数据
func ReadFile(fp string, pos int64, call func([]byte)) (err error) {
	var fi *os.File
	defer func() {
		if fi != nil {
			fi.Close()
		}
	}()

	fi, err = os.OpenFile(fp, os.O_RDONLY, 0666)
	if err != nil {
		return
	}

	// 1.定位,位置有效是否开头读取
	_, err = fi.Seek(pos, io.SeekStart)
	if err != nil {
		return
	}

	// 2.从定位位置开始按行读取文件
	br := bufio.NewReader(fi)
	for {
		a, _, e := br.ReadLine()
		if e != nil {
			if e == io.EOF {
				break
			}
			err = e
			continue
		}
		call(a)
	}

	return nil
}

// config 解析配置数据
func config(fp string) (Config, error) {
	b, err := ioutil.ReadFile(fp)
	if err != nil {
		return Config{}, err
	}
	var conf Config
	_, err = toml.Decode(string(b), &conf)
	if err != nil {
		return Config{}, err
	}
	// 检查下project的值
	// 转为
	d := map[string]*Item{}
	for name, item := range conf.Items {
		item.Parse(name)
		d[name] = item
	}
	return conf, nil
}

// cData 读取缓存数据,并转为Item
func cData(c Config) map[string]*Item {
	dir := c.Server.Dir
	p := processFileName
	if dir != "" {
		p = fmt.Sprintf("%s%c%s", dir, os.PathSeparator, processFileName)
	}
	b, _ := ioutil.ReadFile(p)
	var d map[string]*Item
	if len(b) > 0 {
		_ = json.Unmarshal(b, &d)
	}
	return d
}

// sData 保存数据
func sData(c Config, d []byte) {
	dir := c.Server.Dir
	p := processFileName
	if dir != "" {
		p = fmt.Sprintf("%s%c%s", dir, os.PathSeparator, processFileName)
	}
	err := ioutil.WriteFile(p, d, 0666)
	if err != nil {
		log.Println(err.Error())
		return
	}
}

// DiffData 检查文件
// 1.对比缓存文件和配置文件的差异
// 2.检查文件是否有效,无效则使用新的文件路径
func DiffData(c Config) map[string]*Item {
	// 缓存Item
	cacheItem := cData(c)
	// 配置Item
	confItem := c.Items
	// 需要执行的Item
	runItem := map[string]*Item{}
	for name, fItem := range confItem {
		// a.没有缓存数据
		if cacheItem == nil {
			runItem[name] = fItem
			continue
		}
		// b.缓存数据中无有效数据
		cItem, ok := cacheItem[name]
		if !ok {
			runItem[name] = fItem
			continue
		}
		// c.1 配置项不变,使用缓存数据
		if cItem.Name == fItem.Name &&
			cItem.First == fItem.First &&
			cItem.Regex == fItem.Regex &&
			cItem.Path == fItem.Path {
			runItem[name] = cItem
			continue
		}
		// c.2 否则使用配置文件数据
		runItem[name] = fItem
	}
	// 整理有效数据
	items := map[string]*Item{}
	for name, i := range runItem {
		// a. 检查文件路径
		if i.IsDir {
			p := i.PathDir
			_, err := os.Stat(p)
			if os.IsNotExist(err) {
				log.Printf("name:%s dir:%s error:%s", name, p, err.Error())
				continue
			}
			if i.PathFile != "" {
				_, err = os.Stat(i.PathFile)
				if os.IsNotExist(err) {
					log.Printf("name:%s dir-file:%s error:%s", name, p, err.Error())
					i.PathFile = ""
					i.SeekPos = 0
				}
			}
		} else {
			_, err := os.Stat(i.PathFile)
			if os.IsNotExist(err) {
				log.Printf("name:%s dir-file:%s error:%s", name, i.PathFile, err.Error())
				continue
			}
		}

		// b. 检查pos位置
		if i.SeekPos > 0 {
			if i.IsDir {
				if i.PathFile == "" {
					i.SeekPos = 0
				} else {
					fii, err := os.Stat(i.PathFile)
					if err == nil && fii.Size() < i.SeekPos {
						i.SeekPos = fii.Size()
					}
				}
			} else {
				fii, err := os.Stat(i.PathFile)
				if err == nil && fii.Size() < i.SeekPos {
					i.SeekPos = fii.Size()
				}
			}
		}
		items[name] = i
	}

	return items
}

var PostLinkPathHello = "/log-obs/hello"
var PostLinkPathSend = "/log-obs/send"

// PostLink 发送数据
type PostLink struct {
	Host     string    // host地址
	Protocol string    // tcp|http
	Queue    chan *Msg // 待发送的数据
	done     chan bool // 退出
}

// NewPostLink new postLink
func NewPostLink(host string, proto string) *PostLink {
	p := new(PostLink)
	p.Host = strings.TrimLeft(host, "/")
	p.Protocol = proto
	p.Queue = make(chan *Msg, 10000)
	p.done = make(chan bool)
	return p
}

// AppendItem 添加一条数据
func (_t *PostLink) AppendItem(msg *Msg) {
	_t.Queue <- msg
}

// Send 发送一次数据
func (_t *PostLink) Send() {
	// 1.读取队列中的数据
	l := len(_t.Queue)
	if l == 0 {
		return
	}

	d := map[string][]map[string]interface{}{}
	for i := 0; i < l; i++ {
		m := <-_t.Queue
		ds, _ := d[m.project]
		// TODO:格式化数据
		doc := map[string]interface{}{
			"data": string(m.data),
		}
		ds = append(ds, doc)
		d[m.project] = ds
	}

	var msg string
	for p, c := range d {
		msg += fmt.Sprintf("send to project:%s count:%d ", p, len(c))
	}

	// 2.发送数据,简单的重试
	b, _ := json.Marshal(d)
	for i := 1; i < 4; i++ {
		resp, err := _t.HttpSend(PostLinkPathSend, b)
		if err != nil {
			log.Print("send data error:" + err.Error())
			time.Sleep(time.Second)
			continue
		}
		log.Print(fmt.Sprintf("%s result:%s", msg, string(resp)))
		break
	}
}

// HttpPing 检查网络畅通
func (_t *PostLink) HttpPing() bool {
	d := []byte(`{"app":"log-obs"}`)
	s, err := _t.HttpSend(PostLinkPathHello, d)
	if err != nil {
		log.Println(err.Error())
		return false
	}
	log.Printf("ping datalink rev:%s", string(s))
	return true
}

// HttpSend http协议发送数据
func (_t *PostLink) HttpSend(path string, body []byte) (resp []byte, err error) {
	// 1、创建request
	r := bytes.NewReader(body)
	u := fmt.Sprintf("%s/%s", _t.Host, strings.Trim(path, "/"))
	req, err := http.NewRequest("POST", u, r)
	if err != nil {
		return
	}
	req.Header.Add("Content-Type", "application/json")

	// 2、发送http请求
	client := &http.Client{Timeout: time.Second * 10}
	response, err := client.Do(req)
	if err != nil {
		return
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		err = errors.New("http status err")
		return
	}

	// 3、结果读取
	resp, err = ioutil.ReadAll(response.Body)
	return
}

// Run 运行
func (_t *PostLink) Run() {
	go func() {
		t1 := time.NewTicker(time.Second * 1)
		t2 := time.NewTicker(time.Second * 3)
		defer func() {
			t1.Stop()
			t2.Stop()
			_t.Send()
		}()
		var need bool
		for {
			need = false
			select {
			case <-t1.C:
				need = len(_t.Queue) > 100
			case <-t2.C:
				need = true
			case <-_t.done:
				return
			}
			if need {
				_t.Send()
			}
		}
	}()
}

// Stop 停止任务
func (_t *PostLink) Stop() {
	_t.done <- true
}

// Msg 消息
type Msg struct {
	project string // 项目名称
	data    []byte // 数据部分
}

// NewMsg msg
func NewMsg(project string, data []byte) *Msg {
	m := new(Msg)
	m.project = project
	m.data = data
	return m
}

// Observer 观察者
type Observer struct {
	c    Config            // 配置文件
	r    map[string]*Item  // Item 运行中的任务
	w    *fsnotify.Watcher // inotify watcher
	mu   sync.Mutex        // 锁
	done chan bool         // 退出

	StartAt time.Time  // 启动时间
	Out     chan *Msg  // 消息输出
	Err     chan error // 错误输出
}

// NewWatcher New对象
func NewWatcher(c Config) (*Observer, error) {
	w := new(Observer)
	w.c = c
	w.r = map[string]*Item{}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	w.w = watcher
	w.mu = sync.Mutex{}
	w.done = make(chan bool)
	w.Out = make(chan *Msg)
	w.Err = make(chan error)
	return w, nil
}

// Run 运行任务
func (_t *Observer) Run() {
	go func() {
		_t.StartAt = time.Now()
		for {
			select {
			case event, ok := <-_t.w.Events:
				if !ok {
					return
				}

				// 删除文件|文件夹
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					_t.eventRemoveFile(event.Name)
					continue
				}

				// 创建文件|文件夹.将指针移动到新文件上
				if event.Op&fsnotify.Create == fsnotify.Create {
					_t.eventCreateFile(event.Name)
					continue
				}

				// 向文件中写入内容
				if event.Op&fsnotify.Write == fsnotify.Write {
					_t.eventWriteContent(event.Name)
					continue
				}

			case err, ok := <-_t.w.Errors:
				if !ok {
					return
				}
				_t.Err <- err
			case <-_t.done:
				return
			}
		}
	}()
}

// eventCreateFile 创建文件
func (_t *Observer) eventCreateFile(fp string) {
	// 忽略创建的目录
	ifi, err := os.Stat(fp)
	if err != nil {
		log.Printf("create event error:%s", err.Error())
		return
	}
	if ifi.IsDir() {
		log.Print("create dir, ignore")
		return
	}

	// 监控的目标是否为目录,是目录的话需要更新为具体文件
	idx := strings.LastIndex(fp, string(os.PathSeparator))
	dir := fp[:idx]
	i, ok := _t.r[dir]
	// 没在监控目录里
	if !ok {
		return
	}
	// 不是目录
	if !i.IsDir {
		return
	}
	// 是目录,但是已经监控文件了
	if i.IsDir && i.PathFile != "" {
		return
	}

	// 更新item
	_t.Remove(fp)
	_t.Add(fp, i)
}

// eventRemoveFile 移除文件
func (_t *Observer) eventRemoveFile(fp string) {
	// 移除文件&目录
	log.Printf("remove event,path:%s", fp)

	// 如果是当前监控的文件,则需要一处
	i, ok := _t.r[fp]
	if !ok {
		return
	}
	i.Close()
	_t.Remove(fp)
}

// eventWriteContent 写入文件内容
// 考虑长时间不使用的时候关闭文件
func (_t *Observer) eventWriteContent(fp string) {
	// 从上次读取的位置继续读取文件
	// 读取到文件尾
	i, ok := _t.r[fp]
	if !ok {
		return
	}

	// 读取文件数据
	b, err := i.Read()
	if err != nil {
		log.Printf("write event:%s error:%s", fp, err.Error())
		return
	}
	if b == nil {
		return
	}
	_t.Out <- NewMsg(i.Name, b)
}

// Add 添加监控任务,开启
func (_t *Observer) Add(fp string, p *Item) error {
	_t.mu.Lock()
	_t.r[fp] = p
	_t.mu.Unlock()
	return _t.w.Add(fp)
}

// Remove 移除监控任务
func (_t *Observer) Remove(fp string) error {
	_t.mu.Lock()
	delete(_t.r, fp)
	_t.mu.Unlock()
	return _t.w.Remove(fp)
}

type saveFun func(c Config, d []byte)

// SaveData 保存数据
func (_t *Observer) SaveData(f saveFun) {
	d := map[string]interface{}{}
	for _, item := range _t.r {
		name := item.Name
		d[name] = map[string]interface{}{
			"Name":     item.Name,
			"IsDir":    item.IsDir,
			"PathDir":  item.PathDir,
			"PathFile": item.PathFile,
			"SeekPos":  item.SeekPos,
			"First":    item.First,
			"Regex":    item.Regex,
			"Path":     item.Path,
		}
	}
	b, err := json.Marshal(d)
	if err != nil {
		log.Println(err.Error())
		return
	}
	f(_t.c, b)
	log.Print("save data done!")
}

// CloseItemIfNeed 检查打开的文件
// 超过十分钟,关闭文件
func (_t *Observer) CloseItemIfNeed() {
	n := time.Now()
	for _, item := range _t.r {
		if item.FiFileReader == nil {
			continue
		}
		d := n.Sub(item.LatestTime)
		if d.Minutes() > 10 {
			item.Close()
		}
	}
}

// Stop 退出
func (_t *Observer) Stop() {
	if _t.done == nil {
		return
	}
	<-_t.done
	_t.w.Close()
	close(_t.done)
	close(_t.Out)
	close(_t.Err)
	_t.Out = nil
	_t.Err = nil
	_t.done = nil

	// 关闭文件
	for _, i := range _t.r {
		i.Close()
	}
}

// Config 配置项
type Config struct {
	Server struct {
		Host     string // 远程host
		Dir      string // cache文件目录
		Protocol string // 上传数据协议
	}
	Items map[string]*Item
}

// Item 需要监控的对象
type Item struct {
	// 运行项
	Name         string        // 单元名称
	IsDir        bool          // 是否为文件夹
	PathDir      string        // 文件夹
	PathFile     string        // 文件路径
	SeekPos      int64         // 位置
	FiDir        *os.File      // 文件夹句柄
	FiFile       *os.File      // 文件句柄
	FiFileReader *bufio.Reader // 文件Reader
	LatestTime   time.Time     // 上一次访问时间,长时间不用关闭文件

	// 配置项
	First bool      // 配从第一行数据开始读取
	Regex [2]string // 对数据格式化 ["/(.*?)(.*?)/i", "time:$1,ip:$2,msg:$3"]
	Path  string    // 监控位置
}

// Parse 解析运行参数
func (i *Item) Parse(name string) error {
	i.Name = name
	i.Path = strings.TrimRight(i.Path, string(os.PathSeparator))
	fi, err := os.Open(i.Path)
	if err != nil {
		return err
	}
	defer fi.Close()

	fii, err := fi.Stat()
	if err != nil {
		return err
	}
	i.IsDir = fii.IsDir()
	if i.IsDir {
		i.PathDir = i.Path
		i.PathFile, _ = dirLatestFile(i.PathDir)
	} else {
		i.PathFile = i.Path
		idx := strings.LastIndex(i.Path, string(os.PathSeparator))
		i.PathDir = i.Path[:idx]
	}

	return nil
}

// Read 读取内容
func (i *Item) Read() (bs []byte, err error) {
	i.LatestTime = time.Now()

	fi := i.FiFile
	fr := i.FiFileReader
	if fi == nil {
		fi, err = os.Open(i.PathFile)
		if err != nil {
			return
		}
		fi.Seek(i.SeekPos, io.SeekStart)
		fr = bufio.NewReader(fi)
		i.FiFile = fi
		i.FiFileReader = fr
	}
	b := make([]byte, 1024)
	var n int
	for {
		n, err = i.FiFileReader.Read(b)
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return
		}
		bs = append(bs, b[0:n]...)
	}
	if len(bs) == 0 {
		return
	}

	// 保存文件位置
	fii, err := fi.Stat()
	if err != nil {
		return
	}
	i.SeekPos = fii.Size()
	return
}

// Close 清理资源
func (i *Item) Close() {
	if i.FiDir != nil {
		i.FiDir.Close()
	}
	if i.FiFile != nil {
		i.FiFile.Close()
	}
	i.FiFile = nil
	i.FiDir = nil
	i.FiFileReader = nil
}

// dirLatestFile 从目录下获取最新修改的文件
func dirLatestFile(dir string) (fp string, err error) {
	dirs, err := ioutil.ReadDir(dir)
	if err != nil {
		return "", err
	}
	var m int64
	for _, fii := range dirs {
		curName := fii.Name()
		if curName == "." || curName == ".." {
			continue
		}
		if fii.IsDir() {
			log.Printf("ignore dir:%s subdir:%s", fp, curName)
			continue
		}
		t := fii.ModTime().Unix()
		if t > m {
			m = t
		}
		fp = fmt.Sprintf("%s%c%s", dir, os.PathSeparator, curName)
	}
	return
}
