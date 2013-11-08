package maggiefs

import (

)

var buffPool = make(chan []byte, 512) // up to 512 buffers * 128kb = 64MB approx

func GetBuff() []byte {
	var b []byte
	select {
	case b = <-buffPool:
	// got one off pool
	default:
		// none free, so allocate
		b = make([]byte, 1024*128)
	}
	return b
}

func ReturnBuff(b []byte) {
	// return pipe
	select {
	case buffPool <- b:
		// returned to pool
	default:
		// pool full, GC will handle
	}
}