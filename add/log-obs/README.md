# 日志上传

监控日志,并上传至datalink任务

```json5
{
    // 项目名称
    "project002": {
        // 项目名称
        "Name": "project002",
        // 是否为路径
        "IsDir": true,
        // 文件夹
        "PathDir": "D:\\logs",
        // 具体文件
        "PathFile": "D:\\logs\\app.log.20220704",
        // 文件位置
        "SeekPos": 33,
        // 配置项-从头开始读取
        "First": true,
        // 配置项-格式化读取的数据
        "Regex": ["/time=(.*?), msg=(.*?)/i", "time:$1,msg:$2"],
        // 配置项-路径
        "Path": "D:\\logs"
    }
}
```