package main

import (
	"context"
	"fmt"
	"logcatchsys/DB"
	"logcatchsys/MQ"
	watchconfig "logcatchsys/config"
	"logcatchsys/logtailf"
	"sync"

	"github.com/spf13/viper"
)

var mainOnce sync.Once
var configMap map[string]*watchconfig.ConfigData
var etcdRoot *db.EtcdRoot

func InitConfigMap(configPaths any, keychan chan string, producer *MQ.KafkaProducer) {
	configs := configPaths.(map[string]any)
	for configkey, configvalue := range configs {
		ctx, cancel := context.WithCancel(context.Background())

		configData := &watchconfig.ConfigData{
			ConfigDir:    configkey,
			ConfigPath:  configvalue.(string),
			ConfigCancel: cancel,
		}
		configMap[configkey] = configData

		// 开启goroutine监控日志文件
		go logtailf.WatchLogFile(ctx, configvalue.(string), configkey, keychan, producer)
	}
}

func main() {
	v := viper.New()
	configPaths, ok := watchconfig.ReadConfig(v)
	if configPaths == nil || !ok {
		fmt.Println("读取配置文件失败")
		return
	}

	kafkaProducer := &MQ.KafkaProducer{}
	kafkaProducer.Init("localhost:9092")
	defer kafkaProducer.Close()

	configMap = make(map[string]*watchconfig.ConfigData)
	ctx, cancel := context.WithCancel(context.Background())
	pathChan := make(chan any)
	keychan := make(chan string)

	InitConfigMap(configPaths, keychan, kafkaProducer)
	go watchconfig.WatchConfigFile(v, ctx, pathChan)

	// 初始化 etcd 模块：读取地址和待 watch 的 key 列表
	etcdAddr := v.GetString("etcd.addr")
	etcdKeys := v.GetStringSlice("etcd.logkeys")
	if etcdAddr != "" && len(etcdKeys) > 0 {
		etcdRoot = db.NewEtcdRoot(ctx, etcdAddr, etcdKeys, keychan, kafkaProducer)
		fmt.Println("etcd module started, addr:", etcdAddr, "keys:", etcdKeys)
	}

	// 析构函数
	defer func() {
		mainOnce.Do(func() {
			if err := recover(); err != nil {
				fmt.Println("main goroutine panic:", err)
			}
			cancel()
			for _, oldvalue := range configMap {
				oldvalue.ConfigCancel()
			}
			configMap = nil
			if etcdRoot != nil {
				etcdRoot.Close()
				etcdRoot = nil
			}
		})
	}()

	// 持续等待并处理每次配置变更
	for {
		select {
		case pathData := <-pathChan:
			fmt.Println("收到配置文件变化通知:", pathData)
			pathNew, ok := pathData.(map[string]any)
			if !ok {
				continue
			}

			// 如果旧的配置项在新的配置中不存在，则取消其上下文并从 map 中删除
			for oldkey, oldvalue := range configMap {
				_, ok := pathNew[oldkey]
				if ok {
					continue
				}
				oldvalue.ConfigCancel() // 取消旧的上下文
				delete(configMap, oldkey)
			}

			// 如果新的配置项在旧的配置中不存在，则创建新的上下文并添加到 map 中
			for configkey, configvalue := range pathNew {
				oldvalue, ok := configMap[configkey]
				// 新的配置项在旧的配置中不存在，则创建新的上下文并添加到 map 中
				if !ok {
					ctx, cancel := context.WithCancel(context.Background())
					configData := &watchconfig.ConfigData{
						ConfigDir:    configkey,
						ConfigPath:  configvalue.(string),
						ConfigCancel: cancel,
					}
					configMap[configkey] = configData
					go logtailf.WatchLogFile(ctx, configvalue.(string), configkey, keychan, kafkaProducer)

					continue
				}
				// 如果新的配置项在旧的配置中存在，但值发生了变化，则取消旧的上下文并创建新的上下文
				if oldvalue.ConfigPath != configvalue.(string) {
					oldvalue.ConfigCancel() // 取消旧的上下文

					oldvalue.ConfigPath = configvalue.(string)
					ctx, cancel := context.WithCancel(context.Background())
					oldvalue.ConfigCancel = cancel
					go logtailf.WatchLogFile(ctx, configvalue.(string), configkey, keychan, kafkaProducer)

					continue
				}
			}

			for configkey, configvalue := range configMap {
				fmt.Printf("配置项: %s, 配置值: %s\n", configkey, configvalue.ConfigPath)
			}

		case keypath := <-keychan:
			fmt.Println("收到goroutine崩溃通知:", keypath)
			configData, ok := configMap[keypath]
			if ok {
				fmt.Println("recover goroutine watch ", keypath)
				// 重新开启goroutine监控日志文件
				var ctx context.Context
				ctx, configData.ConfigCancel = context.WithCancel(context.Background())
				go logtailf.WatchLogFile(ctx, configData.ConfigPath, keypath, keychan, kafkaProducer)
				continue
			}
			// 若是 etcd 管理的 key，则通过 etcd watch 自动恢复（无需手动处理）
			if etcdRoot != nil && etcdRoot.HasKey(keypath) {
				fmt.Println("etcd goroutine crash, watch will recover via etcd, key:", keypath)
			}
		}
	}

}
