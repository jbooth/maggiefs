package maggiefs

import (
	"sync"
)

type Generator interface {
	// create a new item for the given key for us to use in the pool
	New(key uint64) (interface{}, error)
	// dispose of the given item
	Dispose(interface{})
}

type KeyedObjPool struct {
	pool           map[uint64]chan interface{}
	l              *sync.RWMutex
	generator      Generator
	maxPerKey      int
	destroyOnError bool
}

func (p *KeyedObjPool) WithObj(key uint64, with func(i interface{}) error) (err error) {
	// get chan
	p.l.RLock()
	ch, exists := p.pool[key]
	p.l.RUnlock()
	if !exists {
		ch = make(chan interface{}, p.maxPerKey)
		p.l.Lock()
		p.pool[key] = ch
		p.l.Unlock()
	}

	// get object
	var i interface{}
	select {
	case i = <-ch:
		// nothing to do
	default:
		// create new one
		i, err = p.generator.New(key)
		if err != nil {
			if p.destroyOnError {
				p.generator.Dispose(i)
			}
			return err
		}
	}

	// do our stuff
	err = with(i)
	if err != nil && p.destroyOnError {
		p.generator.Dispose(i)
		return err
	}
	// return	to pool
	select {
	case ch <- i:
		// successfully added back to freelist
	default:
		// freelist full, dispose of that trash
		p.generator.Dispose(i)
	}
	return err
}
