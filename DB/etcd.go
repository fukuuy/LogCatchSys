package db

import (
	"context"
	"fmt"
	"logcatchsys/MQ"
	"logcatchsys/logtailf"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdConfig 表示单个 etcd key 的运行时状态。
// Path     : etcd 中存储的 key（如 /logs/logdir1）
// LogPath  : etcd key 对应的 value，即要监控的日志文件路径
// Ctx/Cancel: 该 key 的日志监控 goroutine 生命周期
type EtcdConfig struct {
	Path     string
	LogPath  string
	Ctx      context.Context
	Cancel   context.CancelFunc
	KeyChan  chan string
	Producer *MQ.KafkaProducer
}

// EtcdRoot 持有唯一的 etcd client 以及所有被 watch 的 key 状态。
type EtcdRoot struct {
	Client     *clientv3.Client
	ConfigMap  map[string]*EtcdConfig
	mu         sync.RWMutex
	RootCtx    context.Context
	RootCancel context.CancelFunc
}

// InitEtcdClient 创建并返回单一的 etcd client，失败返回 nil 并打印错误。
func InitEtcdClient(etcdAddr string) *clientv3.Client {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{etcdAddr},
	})
	if err != nil {
		fmt.Printf("init etcd client err: %v\n", err)
		return nil
	}
	return cli
}

// NewEtcdRoot 创建 EtcdRoot，持有唯一 client，并为每个 key 启动 watch goroutine。
func NewEtcdRoot(ctx context.Context, cli *clientv3.Client, logkeys []string, keychan chan string, producer *MQ.KafkaProducer) *EtcdRoot {
	rootCtx, rootCancel := context.WithCancel(ctx)
	root := &EtcdRoot{
		Client:     cli,
		ConfigMap:  make(map[string]*EtcdConfig),
		RootCtx:    rootCtx,
		RootCancel: rootCancel,
	}

	for _, key := range logkeys {
		root.AddKey(key, keychan, producer)
	}
	return root
}

// AddKey 新增一个 etcd key 的监控，启动对该 key 的 watch goroutine，并同步维护 ConfigMap。
func (e *EtcdRoot) AddKey(key string, keychan chan string, producer *MQ.KafkaProducer) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.ConfigMap[key]; ok {
		return
	}

	ctx, cancel := context.WithCancel(e.RootCtx)
	cfg := &EtcdConfig{
		Path:     key,
		Ctx:      ctx,
		Cancel:   cancel,
		KeyChan:  keychan,
		Producer: producer,
	}
	e.ConfigMap[key] = cfg

	// 首次读取该 key 当前的值，若有日志路径则启动监控
	if resp, err := e.Client.Get(ctx, key); err == nil && len(resp.Kvs) > 0 {
		e.applyValue(ctx, cfg, string(resp.Kvs[0].Value))
	}

	// 启动对该 key 的 watch goroutine
	go e.watchKey(ctx, cfg)
}

// RemoveKey 取消指定 etcd key 的监控，并从 ConfigMap 中删除。
func (e *EtcdRoot) RemoveKey(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if cfg, ok := e.ConfigMap[key]; ok {
		if cfg.Cancel != nil {
			cfg.Cancel()
		}
		delete(e.ConfigMap, key)
	}
}

// watchKey 监听单个 etcd key 的变更与删除。
func (e *EtcdRoot) watchKey(ctx context.Context, cfg *EtcdConfig) {
	wch := e.Client.Watch(ctx, cfg.Path)
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("etcd watch goroutine exit, key: %s\n", cfg.Path)
			return
		case resp, ok := <-wch:
			if !ok {
				fmt.Printf("etcd watch channel closed, key: %s\n", cfg.Path)
				return
			}
			if err := resp.Err(); err != nil {
				fmt.Printf("etcd watch err: %v, key: %s\n", err, cfg.Path)
				return
			}
			for _, ev := range resp.Events {
				switch ev.Type {
				case mvccpb.PUT:
					// 值变化，更新日志路径并重启监控 goroutine
					newLogPath := string(ev.Kv.Value)
					e.mu.Lock()
					oldCfg, ok := e.ConfigMap[cfg.Path]
					e.mu.Unlock()
					if !ok {
						continue
					}
					e.applyValue(ctx, oldCfg, newLogPath)
				case mvccpb.DELETE:
					// key 被删除，取消监控
					e.mu.Lock()
					if oldCfg, ok := e.ConfigMap[cfg.Path]; ok && oldCfg.Cancel != nil {
						oldCfg.Cancel()
					}
					delete(e.ConfigMap, cfg.Path)
					e.mu.Unlock()
					fmt.Printf("etcd key deleted, key: %s\n", cfg.Path)
				}
			}
		}
	}
}

// applyValue 更新 key 对应的日志路径；若路径发生变化则取消旧 goroutine 并启动新的监控。
func (e *EtcdRoot) applyValue(ctx context.Context, cfg *EtcdConfig, newLogPath string) {
	if cfg.LogPath == newLogPath {
		return
	}

	// 取消旧的日志监控 goroutine
	if cfg.Cancel != nil {
		cfg.Cancel()
	}
	logCtx, logCancel := context.WithCancel(e.RootCtx)

	cfg.LogPath = newLogPath
	cfg.Cancel = logCancel

	if newLogPath == "" {
		fmt.Printf("etcd key value empty, stop watch, key: %s\n", cfg.Path)
		return
	}

	fmt.Printf("start watch log file by etcd, key: %s, path: %s\n", cfg.Path, newLogPath)
	go logtailf.WatchLogFile(logCtx, newLogPath, cfg.Path, cfg.KeyChan, cfg.Producer)
}

// UpdateEtcdLogkeys 根据新的 key 列表同步维护监控（增加/删除），与 main.go 的配置同步逻辑对齐。
func (e *EtcdRoot) UpdateEtcdLogkeys(newKeys []string) {
	newKeyMap := make(map[string]bool)
	for _, k := range newKeys {
		newKeyMap[k] = true
	}

	// 删除新列表中不存在的 key
	e.mu.RLock()
	oldKeys := make([]string, 0, len(e.ConfigMap))
	for k := range e.ConfigMap {
		oldKeys = append(oldKeys, k)
	}
	e.mu.RUnlock()

	for _, oldKey := range oldKeys {
		if !newKeyMap[oldKey] {
			e.RemoveKey(oldKey)
		}
	}

	// 为新增的 key 启动监控
	for _, newKey := range newKeys {
		e.mu.RLock()
		_, exists := e.ConfigMap[newKey]
		e.mu.RUnlock()
		if !exists {
			e.AddKey(newKey, e.getKeyChan(newKey), e.getProducer(newKey))
		}
	}
}

func (e *EtcdRoot) getKeyChan(key string) chan string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if cfg, ok := e.ConfigMap[key]; ok && cfg.KeyChan != nil {
		return cfg.KeyChan
	}
	return nil
}

func (e *EtcdRoot) getProducer(key string) *MQ.KafkaProducer {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if cfg, ok := e.ConfigMap[key]; ok && cfg.Producer != nil {
		return cfg.Producer
	}
	return nil
}

// HasKey 判断指定 etcd key 是否正在被监控。
func (e *EtcdRoot) HasKey(key string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	_, ok := e.ConfigMap[key]
	return ok
}

// Close 取消所有监控并关闭 etcd client。
func (e *EtcdRoot) Close() {
	if e.RootCancel != nil {
		e.RootCancel()
	}
	e.mu.Lock()
	for _, cfg := range e.ConfigMap {
		if cfg.Cancel != nil {
			cfg.Cancel()
		}
	}
	e.mu.Unlock()
	if e.Client != nil {
		e.Client.Close()
	}
}
