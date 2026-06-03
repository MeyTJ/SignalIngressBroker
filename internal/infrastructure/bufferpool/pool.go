package bufferpool

import "sync"

type Pool struct {
	size int
	pool sync.Pool
}

func New(size int) *Pool {
	if size <= 0 {
		size = 4096
	}
	return &Pool{
		size: size,
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, size)
				return b
			},
		},
	}
}

func (p *Pool) Get() []byte {
	b := p.pool.Get().([]byte)
	return b[:0]
}

func (p *Pool) Put(buf []byte) {
	if cap(buf) != p.size {
		return
	}
	p.pool.Put(buf[:0])
}
