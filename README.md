# logcatchsys

一个基于 Go 的分布式日志采集与检索系统。实时采集多个日志文件，将日志发送到 Kafka，并通过独立的 Elasticsearch 进程从 Kafka 消费、解析并写入 Elasticsearch，最终可通过 Kibana 检索。

## 架构

两个独立进程：

1. **主进程**（`main.go`）：多数据源采集 → Kafka
   - 文件配置监听（viper + fsnotify）
   - etcd 配置监控（每个 key 独立 client）
   - tail 日志文件，实时发送到 Kafka
2. **Elasticsearch 进程**（`elasticsearch/main.go`）：Kafka → 解析 → Elasticsearch
   - 消费组方式读取 Kafka
   - 解析日志时间戳与内容
   - 批量写入 ES，按天分索引

## 目录结构

```
logcatchsys/
├── main.go                   # 主进程入口（采集 → Kafka）
├── config/
│   ├── config.yml            # 配置文件
│   └── watchconfig.go        # 配置文件读取与热更新监听
├── DB/
│   └── etcd.go               # etcd 配置监控模块（多 client）
├── MQ/
│   ├── kafkaproducer.go      # Kafka 生产者（同步）
│   └── kafkaconsumer.go      # Kafka 消费者（消费组）
├── logtailf/
│   └── logtailf.go           # 日志文件实时 tail 监控
├── elasticsearch/
│   └── main.go               # 独立 ES 进程（Kafka → ES）
├── writefile/
│   └── writefile.go          # 测试用：生成模拟日志文件
├── docker-compose.yml        # Kafka + Elasticsearch + Kibana
└── go.mod
```

## 环境要求

- Go 1.26+
- Docker + Docker Compose（用于 Kafka / Elasticsearch / Kibana）

## 快速开始

### 1. 启动依赖服务

```bash
docker compose up -d
```

启动完成后：
- Kafka: `localhost:9092`
- Elasticsearch: `localhost:9200`
- Kibana: `localhost:5601`

### 2. 生成测试日志（可选）

```bash
go run ./writefile/
```

该命令会在 `config.yml` 中配置的日志目录生成模拟日志，并将日志文件重命名为时间戳备份。

### 3. 启动主进程（采集 → Kafka）

```bash
go run .
```

### 4. 启动 Elasticsearch 进程（Kafka → ES）

```bash
go run ./elasticsearch
```

### 5. 访问 Kibana

浏览器打开 `http://localhost:5601/app/discover#/`，首次选择 **Explore on my own**，在 Discover 中设置索引模式为 `logs-*` 检索日志。

## 配置说明（config/config.yml）

```yaml
configpath:                     # 要采集的日志文件路径
  logdir1: "../logdir1/log.txt"
  logdir2: "../logdir2/log.txt"
  logdir3: "../logdir3/log.txt"

etcd:                           # etcd 配置监控（可选）
  addr: "localhost:2379"
  logkeys:
    - "/logs/logdir1"
    - "/logs/logdir2"
    - "/logs/logdir3"

kafka:                          # Kafka 配置（ES 进程使用）
  brokers:
    - "localhost:9092"
  topic: "logcatchsys"

consumer:                       # 消费组配置（ES 进程使用）
  group: "elasticsearch-worker"

elasticsearch:                  # Elasticsearch 配置（ES 进程使用）
  addr: "http://localhost:9200"
  index: "logs"                 # 索引前缀，按天分索引：logs-YYYY.MM.DD
```

`configpath` 的路径是相对于运行进程当前工作目录的相对路径。

## 数据流与字段

Kafka 消息（topic: `logcatchsys`）：

| 字段 | 含义 |
|------|------|
| key  | 日志来源标识（如配置项名 `logdir1` 或 etcd key `/logs/logdir1`） |
| value| 日志行文本，如 `[2026-09-03 15:04:05] 这是第 0 条日志` |

ES 文档字段（index: `logs-YYYY.MM.DD`）：

| 字段         | 含义                         |
|--------------|------------------------------|
| timestamp    | 从日志行解析出的时间戳       |
| source       | Kafka key（日志来源标识）    |
| content      | 日志内容                     |
| received_at  | ES 进程收到消息的时间        |

