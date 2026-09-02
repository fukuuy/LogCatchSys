package logconfig

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"path"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

var onceLogConf sync.Once

type ConfigData struct{
	ConfigKey string
	ConfigValue string
	ConfigCancel context.CancelFunc
}

func ReadConfig(v *viper.Viper) (interface{}, bool) {
	v.SetConfigName("config")
	_,filename,_,_ := runtime.Caller(0)
	v.AddConfigPath(path.Dir(filename))

	v.SetConfigType("yaml")
	if err := v.ReadInConfig(); err != nil {
		return nil, false
	}
	configPaths := v.Get("configpath")
	if configPaths == nil {
		return nil, false
	}

	return configPaths, true
}

func WatchConfig(v *viper.Viper, ctx context.Context, pathChan chan interface{}) {
	defer func(){
		onceLogConf.Do(func() {
			fmt.Println("watch config goroutine exit")
			if err := recover(); err != nil {
				fmt.Println("watch config goroutine panic:", err)
			}
			close(pathChan)
		})
	}()

	// 设置监听配置回调函数
	v.OnConfigChange(func(event fsnotify.Event){
		fmt.Printf("config file: %s changed: %s\n", event.Name, event.String())
		configPaths := v.Get("configpath")
		if configPaths == nil {
			return
		}
		pathChan <- configPaths
	})

	// 开始监听配置文件变化
	v.WatchConfig()
	// 信道阻塞，直到接收到取消信号
	<-ctx.Done()
}