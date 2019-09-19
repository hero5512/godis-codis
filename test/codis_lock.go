package main

import (
	"github.com/go-redis/redis"
	"github.com/hero5512/godis-codis/util"
	"sync"
	"time"
)

const (
	LockKey    = "count_key"
	CounterKey = "counter"
)

// Show how a redis distributed lock works
func increace() {
	pool, err := util.GetPool()
	if err != nil {
		panic(err)
	}
	client := pool.GetClient()
	var flag = true
	for flag {
		// lock
		resp := client.SetNX(LockKey, 1, time.Second*2)
		lockSuccess, err := resp.Result()
		if err == nil && lockSuccess {
			println("getlock success")
			flag = false
		} else {
			//println("getlock failed")
		}
	}

	// counter ++
	getResp := client.Get(CounterKey)
	cntValue, err := getResp.Int64()
	println("current counter is:", cntValue)
	if err == redis.Nil {
		resp := client.Set(CounterKey, 1, time.Second*30)
		_, err := resp.Result()
		if err != nil {
			// log err
			println("set value error!")
		}
	} else if err == nil {
		cntValue++
		resp := client.Set(CounterKey, cntValue, time.Second*30)
		_, err := resp.Result()
		if err != nil {
			// log err
			println("set value error!")
		}
	}

	delResp := client.Del(LockKey)
	unlockSuccess, err := delResp.Result()
	if err == nil && unlockSuccess > 0 {
		println("unlock success!")
	} else {
		println("unlock failed", err)
	}

}

func main() {
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			increace()
		}()
	}
	wg.Wait()
	defer util.Close()
}
