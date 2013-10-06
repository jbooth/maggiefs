package mrpc

import (
	"fmt"
	"sync"
)



type Service interface {
	// serves clients, blocking until we either crash or are told to stop
	Serve() error
	// requests stop
	Close() error
	// waits till actually stopped
	WaitClosed() error
}

type MultiService struct {
	clos *sync.Cond
	closed bool
	servs []Service
}

func NewMultiService(servs []Service) Service {
	return &MultiService{ new(sync.Cond), false, servs}
}

func (ms *MultiService) Serve() error {
	errChan := make(chan error, 1)
	for _,s := range ms.servs {
		go func() {
			defer func() {
				if x := recover(); x != nil {
					fmt.Printf("run time panic for service: %v\n", x)
					
					errChan <- fmt.Errorf("Run time panic: %v", x)
					
				}
			}()
			errChan <- s.Serve()
		}()
	}
	return <- errChan
}

func (ms *MultiService) Close() error {
	ms.clos.L.Lock()
	defer ms.clos.L.Unlock()
	var retErr error = nil
	for _,s := range ms.servs {
		err := s.Close()
		if err != nil && retErr == nil {
			retErr = err
		}
		s.WaitClosed()
	}
	ms.closed = true
	return retErr
}

func (ms *MultiService) WaitClosed() error {
	ms.clos.L.Lock()
	for !ms.closed {
		ms.clos.Wait()
	}
	ms.clos.L.Unlock()
	return nil
}