package pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

type Pool struct {
	// 空闲连接队列
	idleConns chan *conn
	// 请求空闲连接队列
	reqConns []chan *conn
	// 最大空闲连接数
	maxIdleConns int
	// 最大连接数
	maxConns int
	// 当前连接数
	curConns int
	// 初始化连接数
	initConns int
	// 连接最大空闲时间
	maxIdleTime time.Duration
	mu          sync.Mutex
	// 连接工厂
	connFactory func() (net.Conn, error)
}

func NewPool(initConns int, maxIdleConns int, maxConns int, connFactory func() (net.Conn, error)) (pool *Pool, err error) {
	if maxIdleConns <= 0 {
		return nil, errors.New("maxIdleConns must be greater than 0")
	}
	if initConns <= 0 {
		return nil, errors.New("initConns must be greater than 0")
	}
	if maxIdleConns < initConns {
		return nil, errors.New("maxIdleConns must be greater than initConns")
	}

	// 初始化pool
	pool = &Pool{
		idleConns:    make(chan *conn, maxIdleConns),
		maxIdleConns: maxIdleConns,
		initConns:    initConns,
		maxConns:     maxConns,
		connFactory:  connFactory,
	}

	// 往pool中添加initConns个连接
	for i := 0; i < initConns; i++ {
		c, err := pool.connFactory()
		if err != nil {
			return nil, err
		}
		pool.idleConns <- &conn{conn: c, lastActiveTime: time.Now()}
	}

	return pool, nil
}

func (pool *Pool) Get(ctx context.Context) (c *conn, err error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:

	}

	// 从空闲连接队列中获取连接
	for {
		select {
		case c = <-pool.idleConns:
			// 有空闲连接
			if c.lastActiveTime.Add(pool.maxIdleTime).Before(time.Now()) {
				// 连接超过最大空闲时间，关闭连接
				_ = c.conn.Close()
				pool.curConns--
				// 继续获取连接(因为队列中可能还有连接)
				continue
			}
			// 没有超过最大空闲时间，返回连接
			return c, nil

		default:
			pool.mu.Lock()
			// 没有空闲连接
			if pool.curConns >= pool.maxConns {

				// 当前连接数大于最大连接数，等待空闲连接
				reqConn := make(chan *conn, 1)
				pool.reqConns = append(pool.reqConns, reqConn)

				pool.mu.Unlock()

				select {
				case c = <-reqConn:
					return c, nil
				case <-ctx.Done():
					// 这里选择不移除reqConn
					go func() {
						c = <-reqConn
						pool.Put(context.Background(), c)
					}()
					return nil, ctx.Err()
				}
			}
			// 当前连接数小于最大连接数，创建新连接
			c.conn, err = pool.connFactory()
			c.lastActiveTime = time.Now()
			if err != nil {
				return nil, err
			}
			pool.curConns++
			return c, nil
		}
	}

}

func (pool *Pool) Put(ctx context.Context, c *conn) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	// 从请求连接队列中获取一个连接
	if len(pool.reqConns) > 0 {
		reqConn := pool.reqConns[0]
		pool.reqConns = pool.reqConns[1:]
		reqConn <- c
		return
	}

	// 没有请求连接，放入空闲连接队列
	select {
	case pool.idleConns <- c:

	default:
		// 空闲连接队列已满，关闭连接
		_ = c.conn.Close()
		pool.curConns--
	}
}

type conn struct {
	// 连接
	conn net.Conn
	// 最后活跃时间
	lastActiveTime time.Time
}
