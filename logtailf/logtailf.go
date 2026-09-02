package logtailf

import (
	"fmt"
	"github.com/hpcloud/tail"
    "logcatchsys/MQ"
    "context"
	"time"
)

func WatchLogFile(ctx context.Context, datapath string, keypath string, keychan chan string, producer *MQ.KafkaProducer) {
	fmt.Println("begin goroutine watch log file ", datapath)

	tailFile,err:=tail.TailFile(datapath,tail.Config{
		//文件被移除或被打包，需要重新打开
        ReOpen: true,
        //实时跟踪
        Follow: true,
        //如果程序出现异常，保存上次读取的位置，避免重新读取
        Location: &tail.SeekInfo{Offset: 0, Whence: 2},
        //支持文件不存在
        MustExist: false,
        Poll:      true,
	})

   if err != nil {
        fmt.Printf("tail file: %s err: %v\n", datapath, err)
        return
    }

    // 协程崩溃时，捕获异常并将keypath发送到keychan
    defer func() {
        if err := recover(); err != nil {
            fmt.Printf("goroutine watch %s panic: %v", keypath, err)
            keychan <- keypath
        }
    }()

    for true {
        select {
        case msg, ok := <-tailFile.Lines:
            if !ok {
                fmt.Printf("tail file close reopen, filename: %s\n", tailFile.Filename)
                time.Sleep(100 * time.Millisecond)
                continue
            }
            //只打印text
            // fmt.Println("msg:", msg.Text)
            producer.Send(keypath, msg.Text)
        case <-ctx.Done():
            fmt.Printf("watch log file goroutine exit, filename: %s\n", datapath)
            return
        }
    }
}