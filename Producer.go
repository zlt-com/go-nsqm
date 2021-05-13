package nsqm

import (
	"github.com/nsqio/go-nsq"
	"github.com/zlt-com/go-config"
	"github.com/zlt-com/go-logger"
)

var (
	producer *nsq.Producer
)

// InitNSQ InitNSQ
func InitNSQ() {
	StartProducter()
}

// StartProducter 初始化生产者
func StartProducter() {
	if producer != nil {
		logger.Info("producer已经初始化")
		return
	}
	var err error
	producer, err = nsq.NewProducer(config.Config.NSQServerIP, nsq.NewConfig())
	logger.Info("初始化producer")
	if err != nil {
		logger.Error(err.Error)
	}
}

// Publish 发布消息
func Publish(topic string, message []byte) (err error) {
	if producer != nil {
		err = producer.Publish(topic, message)
		return err
	}
	return nil
}
