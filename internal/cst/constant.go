package cst

const (
	SyncModeDump    = "dump"    // 以dump方式读取数据
	SyncModeDirect  = "direct"  // 常规查询方式读取数据
	SyncModeStream  = "stream"  // 流读取事件
	SyncModeEmpty   = "empty"   // 不做任何事,只做连接
	SyncModeReplica = "replica" // 副本
)

func InSyncMode(t string) bool {
	switch t {
	case SyncModeDump:
	case SyncModeDirect:
	case SyncModeStream:
	case SyncModeEmpty:
	case SyncModeReplica:
	default:
		return false
	}
	return true
}

const ResourceTypeMongodb = "mongodb"
const ResourceTypeMysql = "mysql"
const ResourceTypeElasticsearch = "elasticsearch"
const ResourceTypePlaintext = "plaintext"
const ResourceTypeRabbitMQ = "rabbitmq"
const ResourceTypeEmpty = "empty"
const ResourceTypeKafka = "kafka"
const ResourceTypeObs = "obs"
const ResourceTypeCanal = "canal"        // 用来检查MySQL日志处理性能
const ResourceTypeErrorOut = "error_out" // 输出到错误日志
const ResourceTypeClickHouse = "clickhouse"

// InResourceType 支持的资源类型
func InResourceType(t string) bool {
	switch t {
	case ResourceTypeMongodb:
	case ResourceTypeMysql:
	case ResourceTypeElasticsearch:
	case ResourceTypePlaintext:
	case ResourceTypeRabbitMQ:
	case ResourceTypeEmpty:
	case ResourceTypeKafka:
	case ResourceTypeObs:
	case ResourceTypeCanal:
	case ResourceTypeErrorOut:
	case ResourceTypeClickHouse:
	default:
		return false
	}
	return true
}

// 任务状态
const (
	TaskNull = iota // 任务创建之后没有做任何操作
	TaskInit        // 任务初始化,检查资源配置项等操作
	TaskDone        // 任务自动结束或者手动结束
	TaskStop        // 任务处于停止状态-任务异常结束
	TaskRun         // 任务运行中
)
