package terms

import (
	"context"
	"data-link-2.0/internal/conf"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	driver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-mysql-org/go-mysql/client"
	elastic7 "github.com/olivere/elastic/v7"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"strings"
	"time"
)

// ElasticSearchConn 连接
func ElasticSearchConn(rc conf.Resource) (*elastic7.Client, error) {
	addr := rc.Dsn
	if addr == "" {
		addr = fmt.Sprintf("%s:%s", rc.Host, rc.Port)
	}
	if !strings.HasPrefix(addr, "http") {
		addr = "http://" + addr
	}
	return elastic7.NewClient(
		elastic7.SetSniff(false),
		elastic7.SetURL(addr),
		elastic7.SetBasicAuth(rc.User, rc.Pass),
	)
}

// MongoDBConn 连接
func MongoDBConn(rc conf.Resource) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	timeout := 30 * time.Second
	opt := options.Client()
	opt.SetConnectTimeout(timeout)
	opt.SetMaxConnIdleTime(timeout)
	opt.SetHeartbeatInterval(timeout)
	opt.SetMaxPoolSize(3)
	opt.SetMinPoolSize(1)
	opt.SetRetryReads(true)

	if rc.Dsn == "" {
		hosts := strings.Split(rc.Host, ",")
		if rc.Port != "" {
			for i, host := range hosts {
				hosts[i] = fmt.Sprintf("%s:%s", host, rc.Port)
			}
		}
		opt.SetHosts(hosts)

		auth := options.Credential{Username: rc.User}
		if rc.Pass != "" {
			auth.Password = rc.Pass
		}
		opt.SetAuth(auth)
	} else {
		opt.ApplyURI(rc.Dsn)
	}
	return mongo.Connect(ctx, opt)
}

// MySQLConn 连接
func MySQLConn(rc conf.Resource) (*client.Conn, error) {
	addr := fmt.Sprintf("%s:%s", rc.Host, rc.Port)
	con, err := client.Connect(addr, rc.User, rc.Pass, "")
	charsetValue, _ := rc.Extra["charset"]
	charset, _ := charsetValue.(string)
	if charset == "" {
		_ = con.SetCharset("utf8mb4")
	} else {
		_ = con.SetCharset(charset)
	}
	return con, err
}

// PlainTextConn 打开文件
func PlainTextConn(rc conf.Resource) (*os.File, error) {
	return os.Open(rc.Dsn)
}

// RabbitMQConn 连接
func RabbitMQConn(rc conf.Resource) (*amqp.Connection, error) {
	addr := rc.Dsn
	if addr == "" { // 不能填写vhost或在port后跟vhost
		addr = fmt.Sprintf("amqp://%s:%s@%s:%s/", rc.User, rc.Pass, rc.Host, rc.Port)
	}
	prop := amqp.NewConnectionProperties()
	prop.SetClientConnectionName("datalink-rabbitmq-conn" + time.Now().Format("2006-01-02 15:04:05"))
	config := amqp.Config{
		Properties: prop,
	}
	return amqp.DialConfig(rc.Dsn, config)
}

// ClickHouseConn 连接
func ClickHouseConn(rc conf.Resource) (driver.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%s", rc.Host, rc.Port)},
			Auth: clickhouse.Auth{
				Username: rc.User,
				Password: rc.Pass,
			},
			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
			DialTimeout: time.Second * 60,
			//TLS: &tls.Config{
			//	InsecureSkipVerify: true,
			//},
		})
	)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			err = fmt.Errorf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}
