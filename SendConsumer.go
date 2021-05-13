package nsqm

import (
	"fmt"
	"time"

	nsq "github.com/nsqio/go-nsq"
	"github.com/zlt-com/go-config"
	"github.com/zlt-com/go-logger"
)

// SendConsumer 发消息消费者
type SendConsumer struct {
}

// HandleMessage 处理消息
func (sc *SendConsumer) HandleMessage(msg *nsq.Message) error {
	fmt.Println("处理消息...", msg)
	return nil
}

// StartConsumer 初始化消费者
func (sc *SendConsumer) StartConsumer() {
	c, err := nsq.NewConsumer(config.Config.NSQTopic, "channel1", nsq.NewConfig()) // 新建一个消费者
	if err != nil {
		logger.Error(err.Error)
	}
	c.SetLogger(nil, 0) //屏蔽系统日志
	c.AddHandler(sc)    // 添加消费者接口

	//建立NSQLookupd连接
	if err := c.ConnectToNSQLookupd(config.Config.NSQLookupd); err != nil {
		logger.Error(err.Error)
	}
	for {
		time.Sleep(time.Second * 20)
	}
}
