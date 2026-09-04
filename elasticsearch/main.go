package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"logcatchsys/MQ"
	watchconfig "logcatchsys/config"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/spf13/viper"
)

// LogMessage 表示一条要写入 Elasticsearch 的日志。
type LogMessage struct {
	Timestamp  time.Time `json:"timestamp"`
	Source     string    `json:"source"`
	Content    string    `json:"content"`
	ReceivedAt time.Time `json:"received_at"`
}

// EsWriter 负责批量写入 Elasticsearch。
type EsWriter struct {
	client *elasticsearch.Client
	index  string

	mu      sync.Mutex
	buf     []LogMessage
	flushCh chan struct{}
	done    chan struct{}
	wg      sync.WaitGroup
}

// parseLog 解析日志行（如 `[2026-09-03 15:04:05] 内容`），返回时间戳与内容。
func parseLog(line string) (time.Time, string) {
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "[") {
		if end := strings.Index(line, "]"); end > 1 {
			tsStr := line[1:end]
			if ts, err := time.Parse("2006-01-02 15:04:05", tsStr); err == nil {
				return ts, strings.TrimSpace(line[end+1:])
			}
		}
	}
	return time.Now(), line
}

// readSettings 读取根 config/config.yml 中的 kafka/consumer/elasticsearch 配置。
func readSettings() (brokers []string, topic, groupID, esAddr, esIndex string) {
	v := viper.New()
	v.SetConfigName(watchconfig.CONFIG_NAME)
	_, filename, _, _ := runtime.Caller(0)
	v.AddConfigPath(path.Dir(filename) + "/../config")
	v.SetConfigType("yaml")
	_ = v.ReadInConfig()

	brokers = v.GetStringSlice("kafka.brokers")
	topic = v.GetString("kafka.topic")
	groupID = v.GetString("consumer.group")
	esAddr = v.GetString("elasticsearch.addr")
	esIndex = v.GetString("elasticsearch.index")
	if esAddr == "" {
		esAddr = "http://localhost:9200"
	}
	if esIndex == "" {
		esIndex = "logs"
	}
	if groupID == "" {
		groupID = "elasticsearch-worker"
	}
	if topic == "" {
		topic = "logcatchsys"
	}
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	return
}

// dailyIndex 按天生成索引名，如 logs-2026.09.03。
func (w *EsWriter) dailyIndex(t time.Time) string {
	return fmt.Sprintf("%s-%s", w.index, t.Format("2006.01.02"))
}

// indexRow 构造 Bulk index 操作的 action 元数据。
func (w *EsWriter) indexRow(msg LogMessage) []byte {
	meta := fmt.Sprintf(`{"index":{"_index":%q}}`, w.dailyIndex(msg.Timestamp))
	data, _ := json.Marshal(msg)
	return []byte(meta + "\n" + string(data) + "\n")
}

// flush 将缓冲中的日志批量写入 ES，必要时按当前索引自动创建索引。
func (w *EsWriter) flush() {
	w.mu.Lock()
	if len(w.buf) == 0 {
		w.mu.Unlock()
		return
	}
	rows := w.buf
	w.buf = nil
	w.mu.Unlock()

	var buf bytes.Buffer
	for _, m := range rows {
		buf.Write(w.indexRow(m))
	}

	// 写入失败重试 3 次
	for attempt := 0; attempt < 3; attempt++ {
		res, err := w.client.Bulk(bytes.NewReader(buf.Bytes()))
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		if res.IsError() {
			res.Body.Close()
			if attempt < 2 {
				time.Sleep(time.Second)
				continue
			}
			break
		}
		// 检查单条 item 是否被拒绝（如数据流只允许 create 操作），避免静默丢数据
		var br struct {
			Errors bool `json:"errors"`
		}
		_ = json.NewDecoder(res.Body).Decode(&br)
		res.Body.Close()
		if br.Errors {
			if attempt < 2 {
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}
}

// backgroundFlush 定时冲洗缓冲。
func (w *EsWriter) backgroundFlush() {
	defer w.wg.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.flush()
		case <-w.flushCh:
			w.flush()
		case <-w.done:
			w.flush()
			return
		}
	}
}

// Add 将日志加入缓冲，达到阈值即触发冲洗。
func (w *EsWriter) Add(msg LogMessage) {
	w.mu.Lock()
	w.buf = append(w.buf, msg)
	throttle := len(w.buf) >= 500
	w.mu.Unlock()
	if throttle {
		select {
		case w.flushCh <- struct{}{}:
		default:
		}
	}
}

// Close 停止后台冲洗并做最终 flush。
func (w *EsWriter) Close() {
	close(w.done)
	w.wg.Wait()
}

func main() {
	brokers, topic, groupID, esAddr, esIndex := readSettings()

	// 初始化 ES client
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{esAddr},
	})
	if err != nil {
		return
	}

	writer := &EsWriter{
		client:  esClient,
		index:   esIndex,
		flushCh: make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	writer.wg.Add(1)
	go writer.backgroundFlush()

	// 初始化 Kafka 消费组
	consumer := &MQ.KafkaConsumer{}
	if err := consumer.Init(brokers, groupID, []string{topic}); err != nil {
		return
	}
	consumer.SetOnMessage(func(key, value []byte) {
		ts, content := parseLog(string(value))
		writer.Add(LogMessage{
			Timestamp:  ts,
			Source:     string(key),
			Content:    content,
			ReceivedAt: time.Now(),
		})
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		consumer.Close()
		writer.Close()
	}()

	// 优雅退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	_ = consumer.Run(ctx)
}
