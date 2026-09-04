package logtailf

import (
	"github.com/hpcloud/tail"
    "logcatchsys/MQ"
    "context"
	"time"
)

func WatchLogFile(ctx context.Context, datapath string, logfilepath string, pathchan chan string, producer *MQ.KafkaProducer) {
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
        return
    }

    // 协程崩溃时，捕获异常并将logfilepath发送到pathchan
    defer func() {
        if err := recover(); err != nil {
            pathchan <- logfilepath
        }
    }()

    for true {
        select {
        case msg, ok := <-tailFile.Lines:
            if !ok {
                time.Sleep(100 * time.Millisecond)
                continue
            }
            producer.Send(logfilepath, msg.Text)
        case <-ctx.Done():
            return
        }
    }
}