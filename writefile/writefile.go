package main

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"sync"
	"time"
	"github.com/spf13/viper"
	"logcatchsys/logconfig"
)

func writeLog(wg *sync.WaitGroup, datapath string) {
	file, err := os.OpenFile(datapath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("open file err:", err)
		return
	}

	defer func() {
		wg.Done()
	}()

	w := bufio.NewWriter(file)
	for i := 0; i < 20; i++ {
		timeStr := time.Now().Format("2006-01-02 15:04:05")
		fmt.Fprintf(w, "[%s] 这是第 %d 条日志\n", timeStr, i)
		time.Sleep(time.Millisecond * 100)
		w.Flush()
	}

	logBak := time.Now().Format("20060102150405") + ".txt"
	logBak = path.Join(path.Dir(datapath), logBak)
	file.Close()
	err = os.Rename(datapath, logBak)
	if err != nil {
		fmt.Println("rename file err:", err)
		return
	}
}

func main() {
	v:=viper.New()
	configPaths, ok := logconfig.ReadConfig(v)
	if configPaths == nil || !ok {
		fmt.Println("读取配置文件失败")
		return
	}

	wg:=&sync.WaitGroup{}
	for _, configvalue := range configPaths.(map[string]any) {
		wg.Add(1)
		go writeLog(wg, configvalue.(string))
	}
	wg.Wait()
}
