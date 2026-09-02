package db

import (
	"context"
	"logcatchsys/MQ"
	"go.etcd.io/etcd/client/v3"
)

type EtcdConfig struct {
	Path string `json:"path"`
	Topic string `json:"topic"`
	Ctx context.Context `json:"-"`
	Cancel context.CancelFunc `json:"-"`
	KeyChan chan string `json:"-"`
	Producer *MQ.KafkaProducer `json:"-"`
}

type EtcdLogMsg struct {
	Ctx context.Context 
	Cancel context.CancelFunc
	KeyChan chan string
	Producer *MQ.KafkaProducer
	EtcdKey string
	EtcdClient *clientv3.Client
	EtcdConfigMap map[string]*EtcdConfig
}

func InitEtcd(etcdDatas interface{}, etcdAddr string, keychan chan string, producer *MQ.KafkaProducer) map[string]*EtcdConfig {
	etcdmap := make(map[string]*EtcdConfig)
	if etcdDatas == nil {
		return etcdmap
	}
	return etcdmap
}
