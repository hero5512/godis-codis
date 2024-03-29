package godis_codis

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	. "github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"sort"
	"sync/atomic"
	"time"
)

// RoundRobinPool is a round-robin redis client pool for connecting multiple codis proxies based on
// zookeeper-go and redis-go.
type RoundRobinPool struct {
	zkConn       *zk.Conn
	zkProxyDir   string
	pools        atomic.Value
	childCh      <-chan zk.Event
	childrenData atomic.Value
	options      redis.Options
	nextIdx      int64
}

// NewRoundRobinPool return a round-robin redis client pool specified by
// zk client and redis options.
func newRoundRobinPool(zkConn *zk.Conn, zkProxyDir string, options redis.Options) (*RoundRobinPool, error) {
	pool := &RoundRobinPool{
		zkConn:     zkConn,
		zkProxyDir: zkProxyDir,
		nextIdx:    -1,
		pools:      atomic.Value{},
	}
	pool.pools.Store([]*PooledObject{})
	_, _, childCh, err := zkConn.ChildrenW(zkProxyDir)
	if err != nil {
		return nil, Wrap(err, fmt.Sprintf("failed to watch %s", zkProxyDir))
	}
	pool.childCh = childCh
	pool.resetPools()

	go pool.watch()
	return pool, nil
}

func (p *RoundRobinPool) resetPools() {
	children, _, err := p.zkConn.Children(p.zkProxyDir)
	if err != nil {
		log.Printf("Failed to get children from %s, %v", p.zkProxyDir, err)
		return
	}
	childrenData := make([]string, 0)
	for _, child := range children {
		data, _, err := p.zkConn.Get(p.zkProxyDir + "/" + child)
		if err != nil {
			log.Printf("Failed to get children data from %s, %v", p.zkProxyDir+"/"+child, err)
			continue
		}
		childrenData = append(childrenData, (string)(data))
	}
	sort.Strings(childrenData)

	pools := p.pools.Load().([]*PooledObject)
	addr2Pool := make(map[string]*PooledObject, len(pools))
	for _, pool := range pools {
		addr2Pool[pool.Addr] = pool
	}
	newPools := make([]*PooledObject, 0)
	for _, childData := range childrenData {
		proxyInfo := ProxyInfo{}
		err := json.Unmarshal([]byte(childData), &proxyInfo)
		if err != nil {
			log.Printf("Parse %s failed", childData)
			continue
		}
		if proxyInfo.State != "online" {
			continue
		}
		addr := proxyInfo.Addr
		if pooledObject, ok := addr2Pool[addr]; ok {
			newPools = append(newPools, pooledObject)
			delete(addr2Pool, addr)
		} else {

			options := p.cloneOptions()
			options.Addr = addr
			options.Network = "tcp"
			pooledObject := NewPooledObject(
				addr,
				redis.NewClient(&options),
			)
			newPools = append(newPools, pooledObject)
			log.Printf("Add new proxy: %s", addr)
		}
	}

	p.pools.Store(newPools)
	for _, pooledObject := range addr2Pool {
		log.Printf("Remove proxy: %s", pooledObject.Addr)
		pooledObject.Client.Close()
	}

}

// GetClient can get a redis client from pool with round-robin policy.
// It's safe for concurrent use by multiple goroutines.
func (p *RoundRobinPool) GetClient() *redis.Client {
	pools := p.pools.Load().([]*PooledObject)
	for {
		current := atomic.LoadInt64(&p.nextIdx)

		var next int64
		if (current) >= (int64)(len(pools))-1 {
			next = 0
		} else {
			next = current + 1
		}
		if atomic.CompareAndSwapInt64(&p.nextIdx, current, next) {
			return pools[next].Client
		}
	}
}

func (p *RoundRobinPool) watch() {
	for {
		select {
		case event := <-p.childCh:
			if event.Path != p.zkProxyDir {
				continue
			}
			log.Printf("Receive child event: type=%s, path=%s, state=%s, err=%v\n",
				event.Type.String(), event.Path, event.State, event.Err)
			if event.Type == zk.EventNodeChildrenChanged {
				p.resetPools()
				_, _, p.childCh, _ = p.zkConn.ChildrenW(p.zkProxyDir)
			}
		}
	}
}

func (p *RoundRobinPool) cloneOptions() redis.Options {
	options := redis.Options{
		Network:            p.options.Network,
		Addr:               p.options.Addr,
		Dialer:             p.options.Dialer,
		OnConnect:          p.options.OnConnect,
		Password:           p.options.Password,
		DB:                 p.options.DB,
		MaxRetries:         p.options.MaxRetries,
		MinRetryBackoff:    p.options.MinRetryBackoff,
		MaxRetryBackoff:    p.options.MaxRetryBackoff,
		DialTimeout:        p.options.DialTimeout,
		ReadTimeout:        p.options.ReadTimeout,
		WriteTimeout:       p.options.WriteTimeout,
		PoolSize:           p.options.PoolSize,
		PoolTimeout:        p.options.PoolTimeout,
		IdleTimeout:        p.options.IdleTimeout,
		IdleCheckFrequency: p.options.IdleCheckFrequency,
		TLSConfig:          p.options.TLSConfig,
	}
	return options
}

// Close closes the pool, releasing all resources except zookeeper client.
func (p *RoundRobinPool) Close() {
	pools := p.pools.Load().([]*PooledObject)
	for _, pool := range pools {
		pool.Client.Close()
	}

}

// Create a RoundRobinPool using the fluent style api.
func Create() *Builder {
	return &Builder{}
}

type Builder struct {
	zkConn           *zk.Conn
	zkProxyDir       string
	zkAddr           []string
	zkSessionTimeout time.Duration
	options          *redis.Options
}

// Set zooKeeper params
func (b *Builder) ZooKeeperClient(zkAddr []string, zkSessionTimeout time.Duration) *Builder {
	b.zkAddr = zkAddr
	b.zkSessionTimeout = zkSessionTimeout
	return b
}

// Set pool config params
func (b *Builder) PoolConfig(options *redis.Options) *Builder {
	b.options = options
	return b
}

// Set codis proxy path on zk.
func (b *Builder) ZkProxyDir(zkProxyDir string) *Builder {
	b.zkProxyDir = zkProxyDir
	return b
}

// validate params
func (b *Builder) validate() (err error) {
	if b.zkProxyDir == "" {
		err = New("zkProxyDir can not be null")
		return
	}
	if b.zkConn == nil {
		if b.zkAddr == nil {
			err = New("zk client can not be null")
		}
		b.zkConn, _, err = zk.Connect(b.zkAddr, b.zkSessionTimeout)
		if err != nil {
			return err
		}
	}

	if b.options == nil {
		b.options = &redis.Options{}
	}
	return nil
}

// create the RoundRobinPool
func (b *Builder) Build() (roundRobinPool *RoundRobinPool, err error) {
	err = b.validate()
	if err != nil {
		return nil, err
	}
	return newRoundRobinPool(b.zkConn, b.zkProxyDir, *b.options)
}
