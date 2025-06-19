package conf

type ServerData struct {
	Data map[string]TaskAutoStart `json:"data"` // 设置自动启动
}

type TaskAutoStart struct {
	TaskID  string `json:"task_id"`
	Running bool   `json:"running"`
}
