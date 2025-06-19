package log

import (
	"fmt"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/writer"
	"os"
	"runtime/debug"
	"strings"
	"time"
)

var logger = logrus.New()

type Format struct{}

func (f Format) Format(entry *logrus.Entry) ([]byte, error) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	msg := fmt.Sprintf("%s [%s] %s\n", timestamp, strings.ToUpper(entry.Level.String()), entry.Message)
	return []byte(msg), nil
}

// InitLogger 默认输出到文件
func InitLogger(dir string, debug bool) {
	// debug 时,作为标准输出
	if debug {
		logger.AddHook(&writer.Hook{
			Writer:    os.Stdout,
			LogLevels: logrus.AllLevels,
		})
	}

	// 输出格式
	logger.SetFormatter(new(Format))
	//logger.SetFormatter(&logrus.JSONFormatter{})

	// 切割文件,保留30天,每天一个新文件
	file := fmt.Sprintf("%sapp.log", dir)
	w, _ := rotatelogs.New(
		file+".%Y%m%d",
		rotatelogs.WithLinkName(file),
		rotatelogs.WithMaxAge(time.Duration(30*24)*time.Hour),
		rotatelogs.WithRotationTime(time.Duration(24)*time.Hour),
	)
	logger.SetOutput(w)
}

func Info(msg ...interface{}) {
	logger.Info(msg...)
}

func Warn(msg ...interface{}) {
	logger.Warn(msg...)
}

func Error(msg ...interface{}) {
	logger.Error(msg...)
	logger.Error(string(debug.Stack()))
}

func Debug(msg ...interface{}) {
	logger.Debug(msg...)
	logger.Debug(string(debug.Stack()))
}

func Trace(msg ...interface{}) {
	logger.Trace(msg...)
}

func Fatal(msg ...interface{}) {
	logger.Fatal(msg)
}
