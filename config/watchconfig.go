package watchconfig

import (
	"context"
	"path"
	"runtime"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

var onceLogConf sync.Once

type ConfigData struct {
	ConfigDir    string
	ConfigPath   string
	ConfigCancel context.CancelFunc
}

var CONFIG_NAME = "config"

func ReadConfig(v *viper.Viper) (any, bool) {
	v.SetConfigName(CONFIG_NAME)
	_, filename, _, _ := runtime.Caller(0)
	v.AddConfigPath(path.Dir(filename))

	v.SetConfigType("yaml")
	if err := v.ReadInConfig(); err != nil {
		return nil, false
	}
	configPath := v.Get("configpath")
	if configPath == nil {
		return nil, false
	}

	return configPath, true
}

func WatchConfigFile(v *viper.Viper, ctx context.Context, pathChan chan any) {
	defer func() {
		onceLogConf.Do(func() {
			close(pathChan)
		})
	}()

	// 设置监听配置文件的回调函数
	v.OnConfigChange(func(event fsnotify.Event) {
		configPath := v.Get("configpath")
		if configPath == nil {
			return
		}
		pathChan <- configPath
	})

	// 开始监听配置文件变化
	v.WatchConfig()
	// 信道阻塞，直到接收到取消信号
	<-ctx.Done()
}
