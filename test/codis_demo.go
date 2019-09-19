package main

import (
	"github.com/go-redis/redis"
	. "github.com/hero5512/godis-codis"
	"time"
)

func main() {
	options := &redis.Options{
		DB:           0,
		PoolSize:     30,
		MinIdleConns: 30,
	}

	zooKeeperServers := []string{"172.168.3.116:2181", "172.168.3.117:2181", "172.168.3.59:2181"}

	pool, err := Create().PoolConfig(options).ZooKeeperClient(zooKeeperServers, 3*time.Second).ZkProxyDir("/jodis/codis-demo").Build()
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	pool.GetClient().Set("k1", "v1", 0)
	v1, err := pool.GetClient().Get("k1").Result()
	if err != nil {
		panic(err)
	}
	println("redis result: " + v1)
}
