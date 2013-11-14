package dataserver

import (
	"sync"
)

// used by the server to manage a stateful write pipeline
type writePipeline struct {
	client Endpoint
	nextInLine Endpoint
	vol *volume
	buff []byte
	
}

func newWritePipeline(client Endpoint, nextInLine Endpoint, firstReq *RequestHeader


type writePipelineClient struct {
	remote Endpoint
	
}

type semaphore struct {
        permits int
        avail   int
        channel chan int
        aMutex  *sync.Mutex
        rMutex  *sync.Mutex
}



func newSem(permits int) *semaphore {
        if permits < 1 {
                panic("Invalid number of permits. Less than 1")
        }
        return &semaphore{
                permits,
                permits,
                make(chan int, permits),
                &sync.Mutex{},
                &sync.Mutex{},
        }
}


//Acquire one permit, if its not available the goroutine will block till its available
func (s *semaphore) acquire() {
        s.aMutex.Lock()
        s.channel <- 1
        s.avail--
        s.aMutex.Unlock()
}


//Release one permit
func (s *semaphore) release() {
        s.rMutex.Lock()
        <-s.channel
        s.avail++
        s.rMutex.Unlock()
}


