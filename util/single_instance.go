package util

import (
	"github.com/go-redis/redis"
	. "github.com/hero5512/godis-codis"
	"sync"
	"time"
)

const (
	ZkProxyDir       = "/jodis/codis-demo"
	ZkSessionTimeout = 3 * time.Second
	PoolSize         = 30
	MinIdleConns     = 30
	DB               = 0
)

var zooKeeperServers = []string{"172.168.3.116:2181", "172.168.3.117:2181", "172.168.3.59:2181"}

var once sync.Once
var pool *RoundRobinPool

// Get pool from codis in single instance manner
func GetPool() (roundRobinPool *RoundRobinPool, err error) {
	once.Do(func() {
		options := &redis.Options{
			DB:           DB,
			PoolSize:     PoolSize,
			MinIdleConns: MinIdleConns,
		}
		roundRobinPool, err = Create().PoolConfig(options).ZooKeeperClient(zooKeeperServers, ZkSessionTimeout).ZkProxyDir(ZkProxyDir).Build()
		pool = roundRobinPool
	})
	return pool, err
}
