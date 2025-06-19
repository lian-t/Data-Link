package linkd

import (
	"context"
	"data-link-2.0/datalink/loop"
	"data-link-2.0/datalink/terms"
	"data-link-2.0/internal/cst"
	"data-link-2.0/internal/log"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-mysql-org/go-mysql/client"
	elastic7 "github.com/olivere/elastic/v7"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"os"
	"strings"
	"time"
)

// 用于输入文件的校验

// ValidTaskMap 校验输入字符串
func ValidTaskMap(c map[string]interface{}) error {

	// 检查setup部分
	err := ValidSetup(c)
	if err != nil {
		return err
	}

	// 检查resource部分
	conn, err := ValidConnects(c)
	if err != nil {
		return err
	}

	// 检查source部分
	smap, err := ValidSource(c, conn)
	if err != nil {
		return err
	}

	// 检查target部分
	err = ValidTarget(c, conn)
	if err != nil {
		return err
	}

	// 检查pipeline部分
	err = ValidPipeline(c, smap)
	if err != nil {
		return err
	}
	return nil
}

// ValidSetup 校验设置
func ValidSetup(s map[string]interface{}) error {
	c, ok := GetMapSI(s, "setup")
	if !ok {
		return errors.New("need setup item")
	}
	desc, ok := GetMapString(c, "desc")
	if !ok || desc == "" {
		return errors.New("need setup->desc item")
	}
	_, ok = GetMapBool(c, "error_record")
	if !ok {
		return errors.New("need setup->error_record item")
	}
	return nil
}

// ValidConnects 校验服务是否可以连接
func ValidConnects(c map[string]interface{}) (map[string]map[string]interface{}, error) {
	arr, ok := GetMapArr(c, "resource")
	if !ok {
		return nil, errors.New("need resource item")
	}
	// 检查 no1000001 不重复,type 不为空
	idMap := map[string]map[string]interface{}{}
	for _, v := range arr {
		var _id string
		r := map[string]interface{}{}

		// 1.检查基础值,转换值
		switch v := v.(type) {
		case map[string]interface{}:
			for k, v1 := range v {
				v2, ok := v1.(string)
				if !ok {
					continue
				}
				switch k {
				case "id":
					_, ok := idMap[v2]
					if ok {
						return nil, errors.New("resource:id must unique")
					}
					_id = v2
				case "type":
					if !cst.InResourceType(v2) {
						return nil, fmt.Errorf("resource type:%s not support", v2)
					}
				default:
				}
				r[k] = v2
			}
		}
		// 2. 检查连通性
		var err error
		rt, _ := GetMapString(r, "type")
		switch rt {
		case cst.ResourceTypeEmpty:
			// pass 不做任何检查
		case cst.ResourceTypeElasticsearch:
			// 检查elastic是否连通
			err = ValidConnectES(r)
		case cst.ResourceTypeMongodb:
			// 检查mongodb的连通性
			err = ValidConnectMongoDB(r)
		case cst.ResourceTypeMysql:
			// 检查mysql的连通性
			err = ValidConnectMysql(r)
		case cst.ResourceTypePlaintext:
			// 检查文件是否能操作
			err = ValidConnectPlaintext(r)
		case cst.ResourceTypeRabbitMQ:
			// 检查rabbitmq能不能登录
			err = ValidConnectRabbitMQ(r)
		case cst.ResourceTypeKafka:
			// 检查kafka能不能登录
			err = ValidConnectKafka(r)
		case cst.ResourceTypeObs:
			// TODO:需要检查是否开启log-obs服务
		case cst.ResourceTypeCanal:
			// 检查mysql的连通性
			err = ValidConnectMysql(r)
		case cst.ResourceTypeErrorOut:
			// pass
		case cst.ResourceTypeClickHouse:
			err = ValidConnectClickHouse(r)
		}
		if err != nil {
			bts, _ := json.Marshal(r)
			msg := fmt.Sprintf("resource connect:\nlink:%s\nerror:%s", string(bts), err.Error())
			return nil, errors.New(msg)
		}
		// 保存基本信息之外,保存连接信息等
		r["conn"] = nil
		idMap[_id] = r

	}
	return idMap, nil
}

// ValidConnectES 检查ES
func ValidConnectES(c map[string]interface{}) error {
	log.Info("ValidConnectES")
	addr, ok := GetMapString(c, "dsn")
	if !ok || addr == "" {
		host, _ := GetMapString(c, "host")
		port, _ := GetMapString(c, "port")
		addr = host + ":" + port
	}
	user, _ := GetMapString(c, "user")
	pass, _ := GetMapString(c, "pass")
	log.Info(addr)
	log.Info(user)
	log.Info(pass)

	es7, err := elastic7.NewClient(
		elastic7.SetSniff(false),
		elastic7.SetURL(addr),
		elastic7.SetBasicAuth(user, pass),
	)
	defer func() {
		if es7 != nil {
			es7.Stop()
		}
	}()
	if err != nil {
		return err
	}
	return nil
}

// ValidConnectMongoDB 检查mongodb
func ValidConnectMongoDB(c map[string]interface{}) error {
	timeout := 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	opt := options.Client()
	opt.SetConnectTimeout(timeout)
	opt.SetMaxConnIdleTime(timeout)
	opt.SetSocketTimeout(timeout)
	opt.SetMaxPoolSize(3)
	opt.SetMinPoolSize(1)
	opt.SetRetryReads(true)
	opt.SetHeartbeatInterval(timeout)

	addr, _ := GetMapString(c, "dsn")
	if addr == "" {
		host, _ := GetMapString(c, "host")
		port, _ := GetMapString(c, "port")
		opt.SetHosts([]string{fmt.Sprintf("%s:%s", host, port)})

		user, _ := GetMapString(c, "user")
		pass, _ := GetMapString(c, "pass")
		auth := options.Credential{Username: user}
		if pass != "" {
			auth.Password = pass
		}
		opt.SetAuth(auth)
	} else {
		opt.ApplyURI(addr)
	}
	conn, err := mongo.Connect(ctx, opt)
	defer func() {
		if conn != nil {
			conn.Disconnect(ctx)
		}
		cancel()
	}()
	if err != nil {
		return err
	}
	err = conn.Ping(ctx, readpref.Nearest())
	if err != nil {
		return err
	}
	return nil
}

// ValidConnectMysql 检查MySQL
func ValidConnectMysql(c map[string]interface{}) error {

	host, _ := GetMapString(c, "host")
	port, _ := GetMapString(c, "port")
	user, _ := GetMapString(c, "user")
	pass, _ := GetMapString(c, "pass")
	addr := fmt.Sprintf("%s:%s", host, port)

	conn, err := client.Connect(addr, user, pass, "")
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	if err != nil {
		return err
	}
	err = conn.Ping()
	if err != nil {
		return err
	}
	return nil
}

// ValidConnectPlaintext 检查纯文本
func ValidConnectPlaintext(c map[string]interface{}) error {
	dsn, _ := GetMapString(c, "dsn")
	fi, err := os.OpenFile(dsn, os.O_CREATE|os.O_APPEND, 0644)
	defer func() {
		if fi != nil {
			fi.Close()
		}
	}()
	if err != nil {
		return err
	}
	return nil
}

// ValidConnectRabbitMQ 检查RabbitMQ
func ValidConnectRabbitMQ(c map[string]interface{}) error {
	addr, _ := GetMapString(c, "dsn")
	if addr == "" {
		user, _ := GetMapString(c, "user")
		pass, _ := GetMapString(c, "pass")
		host, _ := GetMapString(c, "host")
		port, _ := GetMapString(c, "port")
		vhost, _ := GetMapString(c, "vhost")
		addr = fmt.Sprintf("amqp://%s:%s@%s:%s/%s", user, pass, host, port, vhost)
	}
	conn, err := amqp.Dial(addr)
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	if err != nil {
		return err
	}
	return nil
}

// ValidConnectKafka 检查kafka
func ValidConnectKafka(c map[string]interface{}) error {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}
	addr, _ := GetMapString(c, "dsn")
	if addr == "" {
		host, _ := GetMapString(c, "host")
		port, _ := GetMapString(c, "port")
		addr = fmt.Sprintf("%s:%s", host, port)
	}
	user, _ := GetMapString(c, "user")
	pass, _ := GetMapString(c, "pass")
	if user != "" && pass != "" {
		// 只支持plain认证
		dialer.SASLMechanism = plain.Mechanism{
			Username: user,
			Password: pass,
		}
	}
	_, err := dialer.Dial("tcp", addr)
	return err
}

// ValidConnectClickHouse 检查clickhouse
func ValidConnectClickHouse(c map[string]interface{}) error {
	host, _ := GetMapString(c, "host")
	port, _ := GetMapString(c, "port")
	user, _ := GetMapString(c, "user")
	pass, _ := GetMapString(c, "pass")
	addr := fmt.Sprintf("%s:%s", host, port)
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{addr},
			Auth: clickhouse.Auth{
				Username: user,
				Password: pass,
			},
			//TLS: &tls.Config{
			//	InsecureSkipVerify: true,
			//},
		})
	)
	if err != nil {
		return err
	}
	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			err = fmt.Errorf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return err
	}
	_ = conn.Close()
	return nil
}

// ValidSource 校验source参数
func ValidSource(c map[string]interface{}, conn map[string]map[string]interface{}) (map[string]map[string]interface{}, error) {
	arr, ok := GetMapArr(c, "source")
	if !ok {
		return nil, errors.New("need source item")
	}

	hasSource := false
	smap := map[string]map[string]interface{}{}
	for _, v := range arr {
		switch v := v.(type) {
		case map[string]interface{}:
			// 检查不识别的参数
			for k1, _ := range v {
				switch k1 {
				case "resource_id", "sync_mode", "document_set", "extra":
				default:
					msg := fmt.Sprintf("source unsupport params:%s", k1)
					return nil, errors.New(msg)
				}
			}

			// 检查 resource_id
			ridv, ok := v["resource_id"]
			if !ok {
				return nil, errors.New("source resource_id can't empty")
			}
			resourceId, _ := ridv.(string)
			if resourceId == "" {
				return nil, errors.New("source resource_id can't empty")
			}

			resource, ok := conn[resourceId]
			if !ok {
				return nil, errors.New("source resource_id not in resource set")
			}

			// 检查 sync_mode
			syncMode, ok := GetMapString(v, "sync_mode")
			if !ok || !cst.InSyncMode(syncMode) {
				return nil, errors.New("数据输入源同步模式不支持")
			}
			// source 只支持单一输入源
			if hasSource && syncMode != cst.SyncModeEmpty {
				return nil, errors.New("数据输入源只支持单一源")
			}
			hasSource = true

			// syncMode=empty 时,不检查 document_set
			if syncMode != cst.ResourceTypeEmpty {
				// 检查 document_set,两种格式:
				// 1 "document_set":"document_set_name1"
				// 2.1 "document_set":{"document_set_name1":"document_set_name_alias"}
				// 2.2 "document_set":{"document_set_name1":["document_set_name_alias1","document_set_name_alias2"]}
				documentSetValueOrigin, _ := v["document_set"]

				var documentSetValue map[string]interface{}
				switch documentSetValueOrigin.(type) {
				case string:
					documentSetValue = map[string]interface{}{
						documentSetValueOrigin.(string): documentSetValueOrigin,
					}
				}
				if documentSetValue == nil {
					documentSetValue, _ = GetMapSI(v, "document_set")
				}
				for _, docSetAlias := range documentSetValue {
					switch aliasList := docSetAlias.(type) {
					case string:
						if aliasList == "" {
							return nil, errors.New("source document_set need set alias")
						}
						list := strings.Split(aliasList, ",")
						for _, s := range list {
							smap[s] = v
						}
					case []interface{}:
						for _, alias := range aliasList {
							alias, _ := alias.(string)
							smap[alias] = v
						}
					default:
						return nil, errors.New("source document_set format ,string or []string")
					}
				}
			}

			// 检查resource支持的类型
			var supportMode map[string]bool
			resourceType, _ := GetMapString(resource, "type")
			switch resourceType {
			case cst.ResourceTypeMongodb:
				supportMode = map[string]bool{
					cst.SyncModeDirect:  true,
					cst.SyncModeStream:  true,
					cst.SyncModeEmpty:   true,
					cst.SyncModeReplica: true,
				}

				// 检查extra参数
				// streamAt
				// direct_resume_objectId
				// resume
				extra, ok := GetMapSI(v, "extra")
				if ok {
					for k2, _ := range extra {
						switch k2 {
						case "streamAt":
							v3, ok := GetMapFloat64(extra, "streamAt")
							if !ok || v3 == 0 {
								return nil, errors.New("source extra:resume value error, must number")
							}
						case "resume":
							_, ok = GetMapBool(extra, "resume")
							if !ok {
								return nil, errors.New("source extra:resume value error, must boolean")
							}
						case "direct_resume":
							_, ok = GetMapBool(extra, "direct_resume")
							if !ok {
								return nil, errors.New("source extra:resume value error, must boolean")
							}
						default:
							msg := fmt.Sprintf("source extra unsupport params:%s", k2)
							return nil, errors.New(msg)
						}
					}
				}
			case cst.ResourceTypeMysql:
				// 当source为MySQL,sync_mode=stream或replica时,检查开启bin_log,格式为row.
				switch syncMode {
				case cst.SyncModeStream, cst.SyncModeReplica:
					a := conn[resourceId]
					err := mysqlValidBinlog(a)
					if err != nil {
						return nil, err
					}
				}
				supportMode = map[string]bool{
					cst.SyncModeDump:    true,
					cst.SyncModeDirect:  true,
					cst.SyncModeStream:  true,
					cst.SyncModeEmpty:   true,
					cst.SyncModeReplica: true,
				}
				extra, ok := GetMapSI(v, "extra")
				if ok {
					for k2, _ := range extra {
						switch k2 {
						case "blHeader":
							blHeader, _ := extra["blHeader"]
							_, ok := blHeader.(bool)
							if !ok {
								return nil, errors.New("source extra:blHeader value error, must boolean")
							}
						case "onDDL":
							ddlValue, _ := extra["onDDL"]
							_, ok := ddlValue.(bool)
							if !ok {
								return nil, errors.New("source extra:onDDL value error, must boolean")
							}
						case "resume":
							_, ok := GetMapBool(extra, "resume")
							if !ok {
								return nil, errors.New("source extra:resume value error, must boolean")
							}
						case "limit":
							_, ok := GetMapFloat64(extra, "limit")
							if !ok {
								return nil, errors.New("source extra:limit value error, must number")
							}
						case "cdc_type":
							cdcType, _ := GetMapString(extra, "cdc_type")
							if cdcType == "" {
								return nil, errors.New("source extra:cdc_type value error, must number")
							}
							val, _ := extra["cdc_field_interval"]
							fieldInterval, _ := val.(float64)
							if fieldInterval == 0 {
								return nil, errors.New("source extra:cdc_field_name value error, must number")
							}
							val, _ = extra["cdc_field_insert"]
							fieldName, _ := val.(string)
							if fieldName == "" {
								return nil, errors.New("source extra:cdc_field_insert value error, must string")
							}
							val, _ = extra["cdc_field_update"]
							fieldName, _ = val.(string)
							if fieldName == "" {
								return nil, errors.New("source extra:cdc_field_update value error, must string")
							}
						case "cdc_field_insert", "cdc_field_update", "cdc_field_delete", "cdc_field_interval":
						case "read_type", "sql":
						default:
							msg := fmt.Sprintf("source extra unsupport params:%s", k2)
							return nil, errors.New(msg)
						}
					}
				}
			case cst.ResourceTypeElasticsearch:
				supportMode = map[string]bool{
					cst.SyncModeDirect: true,
					cst.SyncModeEmpty:  true,
				}
			case cst.ResourceTypePlaintext:
				supportMode = map[string]bool{
					cst.SyncModeDirect: true,
					cst.SyncModeEmpty:  true,
				}
			case cst.ResourceTypeRabbitMQ:
				supportMode = map[string]bool{
					cst.SyncModeStream: true,
					cst.SyncModeEmpty:  true,
				}
			case cst.ResourceTypeKafka:
				supportMode = map[string]bool{
					cst.SyncModeStream:  true,
					cst.SyncModeReplica: true,
				}
				extra, ok := GetMapSI(v, "extra")
				if ok {
					for k2, _ := range extra {
						switch k2 {
						case "offset":
							_, ok := extra["offset"].(float64)
							if !ok {
								return nil, errors.New("source extra:offset value error, must number")
							}
						default:
							msg := fmt.Sprintf("source extra unsupport params:%s", k2)
							return nil, errors.New(msg)
						}
					}
				}
			case cst.ResourceTypeObs:
				supportMode = map[string]bool{
					cst.SyncModeStream: true,
				}
			case cst.ResourceTypeCanal:
				supportMode = map[string]bool{
					cst.SyncModeStream: true,
				}
			case cst.ResourceTypeClickHouse:
				supportMode = map[string]bool{
					cst.SyncModeDirect: true,
					cst.SyncModeEmpty:  true,
				}
				extra, _ := GetMapSI(v, "extra")
				if len(extra) > 0 {
					for k2, _ := range extra {
						switch k2 {
						case "limit":
							_, ok = GetMapFloat64(extra, "limit")
							if !ok {
								return nil, errors.New("source extra:limit value error, must number")
							}
						default:
							msg := fmt.Sprintf("source extra unsupport params:%s", k2)
							return nil, errors.New(msg)
						}
					}
				}
			}
			_, ok = supportMode[syncMode]
			if !ok {
				msg := fmt.Sprintf("source resourceType:%s sync_mode:%s not support", resourceType, syncMode)
				return nil, errors.New(msg)
			}
		}
	}
	return smap, nil
}

// ValidTarget 校验target参数
func ValidTarget(c map[string]interface{}, conn map[string]map[string]interface{}) error {
	arr, ok := GetMapArr(c, "target")
	if !ok {
		return errors.New("need source item")
	}
	for _, v := range arr {
		switch v := v.(type) {
		case map[string]interface{}:
			// 检查不识别的参数
			for k1, _ := range v {
				switch k1 {
				case "resource_id", "document_set", "extra":
				default:
					msg := fmt.Sprintf("target unsupport params:%s", k1)
					return errors.New(msg)
				}
			}
			// 检查 resource_id
			resourceIdStr, _ := v["resource_id"]
			resourceId, _ := resourceIdStr.(string)
			resource, ok := conn[resourceId]
			if !ok {
				return errors.New("target resource_id not in resource set")
			}

			// 检查 document_set
			documentSet, ok := GetMapSS(v, "document_set")
			if !ok || len(documentSet) == 0 {
				return errors.New("target document_set can't empty")
			}

			// 检查 extra 数据
			// elasticsearch 有 limit 选项
			resourceType, _ := GetMapString(resource, "type")
			extra, _ := GetMapSI(v, "extra")
			switch resourceType {
			case cst.ResourceTypeObs:
				// pass
				return errors.New("target type error")
			case cst.ResourceTypeKafka:
				// 检查topic
				for k3, v3 := range extra {
					switch k3 {
					case "format":
						_, ok := v3.(bool)
						if !ok {
							return errors.New("target extra params:format value invalidation")
						}
					case "auto_topic_create":
						_, ok := v3.(bool)
						if !ok {
							return errors.New("target extra params:auto_topic_create on invalid")
						}
					default:
						return fmt.Errorf("target extra params:%s unrecognized", k3)
					}
				}
			case cst.ResourceTypeEmpty:
				// pass 不做任何检查
			case cst.ResourceTypeElasticsearch:
				for k3, v3 := range extra {
					switch k3 {
					case "auto_bulk":
						_, ok = v3.(bool)
						if !ok {
							return errors.New("target extra params:auto_bulk type need bool")
						}
					case "auto_bulk_speed":
						_, ok = v3.(float64)
						if !ok {
							return errors.New("target extra params:auto_bulk_speed type need integer")
						}
					case "limit":
						switch v3 := v3.(type) {
						case float64:
							if v3 < 1 {
								return errors.New("target extra params:limit greater than 1")
							}
						default:
							return errors.New("target extra params:limit value error")
						}
					default:
						return fmt.Errorf("target extra params:%s unrecognized", k3)
					}
				}
			case cst.ResourceTypeMongodb:
				// 不支持参数
				for k3, _ := range extra {
					msg := fmt.Sprintf("target type=%s, extra unsupport params:%s", resourceType, k3)
					return errors.New(msg)
				}
			case cst.ResourceTypeMysql:
				// 不支持参数
				for k3, _ := range extra {
					msg := fmt.Sprintf("target type=%s, extra unsupport params:%s", resourceType, k3)
					return errors.New(msg)
				}
			case cst.ResourceTypePlaintext:
				for k3, _ := range extra {
					switch k3 {
					case "append":
						_, ok := GetMapBool(extra, k3)
						if !ok {
							return errors.New("target extra params:append value is boolean")
						}
					case "created":
						_, ok := GetMapBool(extra, k3)
						if !ok {
							return errors.New("target extra params:created value is boolean")
						}
					default:
						msg := fmt.Sprintf("target extra unsupport params:%s", k3)
						return errors.New(msg)
					}
				}
			case cst.ResourceTypeRabbitMQ:
				// 必须有参数
				v3, ok := GetMapString(extra, "exchange")
				if !ok || v3 == "" {
					msg := fmt.Sprintf("target type=%s, params:extra=>exchange must not empty string", resourceType)
					return errors.New(msg)
				}
				// pass
			case cst.ResourceTypeErrorOut:
				// pass
			case cst.ResourceTypeClickHouse:
				// pass
			default:
				return errors.New("不支持的终端")
			}
		}
	}
	return nil
}

// ValidPipeline pipeline的校验
func ValidPipeline(c map[string]interface{}, smap map[string]map[string]interface{}) error {
	arr, ok := GetMapArr(c, "pipeline")
	if !ok {
		return errors.New("need pipeline item")
	}
	for _, v := range arr {
		switch v := v.(type) {
		case map[string]interface{}:
			documentSetValue, ok := GetMapString(v, "document_set")
			documentSet := strings.Split(documentSetValue, ",")
			if len(documentSet) < 1 {
				return errors.New("pipeline document_set can't empty")
			}
			for _, docSet := range documentSet { // 检查source中document_set是否有效
				if docSet == "*" {
					continue
				}
				_, ok = smap[docSet]
				if !ok {
					return fmt.Errorf("pipeline document_set:%s can't find in source", docSet)
				}
			}

			flow, _ := GetMapArr(v, "flow")
			if len(flow) == 0 {
				return fmt.Errorf("pipeline document_set:%s, flow can't empty", documentSetValue)
			}
			err := ValidPipelineFlow(flow, smap)
			if err != nil {
				return fmt.Errorf("pipeline document_set:%s, error:%s", documentSetValue, err.Error())
			}
		}
	}
	return nil
}

// ValidPipelineFlow 校验flow
func ValidPipelineFlow(flow []interface{}, smap map[string]map[string]interface{}) error {
	for i, v := range flow {
		switch v := v.(type) {
		case map[string]interface{}:
			t, _ := GetMapString(v, "type")
			switch t {
			case "map", "filter", "proc":
				script, _ := GetMapString(v, "script")
				if script == "" {
					return fmt.Errorf("position index:%d, script can't be empty", i)
				}
				_, err := loop.NewScript(script, nil)
				if err != nil {
					return fmt.Errorf("position index:%d, script error:%s", i, err.Error())
				}
			case "relateInline":
				// 关联类型
				assocType, ok := GetMapString(v, "assoc_type")
				if !ok || assocType == "" || (assocType != "11" && assocType != "1n") {
					return errors.New("relateInline:assoc_type value only '11' or '1n'")
				}
				// 关联数据集
				relateDocumentSet, ok := GetMapString(v, "relate_document_set")
				if !ok || relateDocumentSet == "" {
					return errors.New("relateInline:relate_document_set can't be empty")
				}
				// 关联层级:sub子集,同级sib
				layerType, ok := GetMapString(v, "layer_type")
				if !ok || layerType == "" || (layerType != "sub" && layerType != "sib") {
					return errors.New("relateInline:layer_type value only 'sub' or 'sib'")
				}
				// 当为子集时,key名称 sub_label
				if layerType == "sub" {
					subLabel, ok := GetMapString(v, "sub_label")
					if !ok || subLabel == "" {
						return errors.New("relateInline:when layer_type=sub and sub_label can't be empty")
					}
				}
				// 当为同级时,field_map 字段映射,其他...
				if layerType == "sib" {
					_, ok := GetMapSI(v, "field_map")
					if !ok {
						return errors.New("relateInline:when layer_type=sib and field_map must be mapping")
					}
				}
				// where 关联关系,条件不能为空
				wheres, _ := GetMapArr(v, "wheres")
				if len(wheres) == 0 {
					return errors.New("relateInline:wheres can't be empty")
				}
				// 检查id是否在source中
				// "relate_resource_id": "no1000001"
				//relateResourceId, _ := GetMapString(v, "relate_resource_id")
				//for _, sourceMap := range smap {
				//	_, ok = sourceMap[relateResourceId]
				//	if !ok {
				//		return fmt.Errorf("relateInline relate_resource_id:%s can't be find in resource", relateResourceId)
				//	}
				//}
			case "mapInline":
				fieldMap, ok := GetMapArr(v, "field_map")
				if !ok || len(fieldMap) < 1 {
					return errors.New("mapInline field_map must be set value")
				}
				for _, fieldItem := range fieldMap {
					fieldItem, _ := fieldItem.(map[string]interface{})
					if len(fieldItem) < 4 {
						return errors.New("mapInline field_map format error")
					}
					temp, _ := GetMapString(fieldItem, "srcField")
					if temp == "" {
						return errors.New("mapInline srcField not empty")
					}
					temp, _ = GetMapString(fieldItem, "srcType")
					if temp == "" {
						return errors.New("mapInline srcType not empty")
					}
					temp, _ = GetMapString(fieldItem, "aimField")
					if temp == "" {
						return errors.New("mapInline aimField not empty")
					}
					temp, _ = GetMapString(fieldItem, "aimType")
					if temp == "" {
						return errors.New("mapInline aimType not empty")
					}
				}
			default:
				return fmt.Errorf("pipeline flow item type:%s unsupported ", t)
			}
		}
	}
	return nil
}

// GetMapFloat64 float64
func GetMapFloat64(c map[string]interface{}, n string) (float64, bool) {
	v, ok := c[n]
	if !ok {
		return 0, false
	}
	switch v := v.(type) {
	case float64:
		return v, true
	}
	return 0, false
}

// GetMapBool bool
func GetMapBool(c map[string]interface{}, n string) (bool, bool) {
	v, ok := c[n]
	if !ok {
		return false, false
	}
	switch v := v.(type) {
	case bool:
		return v, true
	}
	return false, false
}

// GetMapString string
func GetMapString(c map[string]interface{}, n string) (string, bool) {
	v, ok := c[n]
	if !ok {
		return "", false
	}
	switch v := v.(type) {
	case string:
		return v, true
	}
	return "", false
}

// GetMapSS map[string]string
func GetMapSS(c map[string]interface{}, n string) (map[string]string, bool) {
	v, ok := c[n]
	if !ok {
		return nil, false
	}
	switch v1 := v.(type) {
	case map[string]interface{}:
		v3 := map[string]string{}
		for k2, v2 := range v1 {
			switch v2 := v2.(type) {
			case string:
				v3[k2] = v2
			default:
			}
		}
		return v3, true
	case map[string]string:
		return v1, true
	}
	return nil, false
}

// GetMapSI map[string]interface{}
func GetMapSI(c map[string]interface{}, n string) (map[string]interface{}, bool) {
	v, ok := c[n]
	if !ok {
		return nil, false
	}
	switch v := v.(type) {
	case map[string]interface{}:
		return v, true
	}
	return nil, false
}

// GetMapArr 获取[]interface{}
func GetMapArr(c map[string]interface{}, n string) ([]interface{}, bool) {
	v, ok := c[n]
	if !ok {
		return nil, false
	}
	switch v := v.(type) {
	case []interface{}:
		return v, true
	}
	return nil, false
}

// mysqlValidBinlog 检查binlog格式
func mysqlValidBinlog(c map[string]interface{}) error {
	host, _ := GetMapString(c, "host")
	port, _ := GetMapString(c, "port")
	user, _ := GetMapString(c, "user")
	pass, _ := GetMapString(c, "pass")
	addr := fmt.Sprintf("%s:%s", host, port)

	conn, err := client.Connect(addr, user, pass, "")
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	if err != nil {
		return err
	}
	ret, err := conn.Execute("SHOW VARIABLES LIKE 'log_bin'")
	if err != nil {
		return err
	}
	arr := terms.MySQLReadResultToSlice(ret)
	if len(arr) != 1 {
		return errors.New("log_bin check error")
	}
	if arr[0]["Value"] != "ON" {
		return errors.New("log_bin need ON")
	}
	return nil
}
