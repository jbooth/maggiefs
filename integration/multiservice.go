package integration

import (
	"fmt"
	"sync"
)

// utility to wrap multiple services

type MultiService struct {
	clos    *sync.Cond
	closed  bool
	servs   []Service
	errChan chan error
}

func NewMultiService() *MultiService {
	return &MultiService{sync.NewCond(&sync.Mutex{}), false, make([]Service, 0, 0), make(chan error, 1)}
}

// adds and starts a service.  our Serve() method returns on any error
func (ms *MultiService) AddService(s Service) error {
	if ms == nil {
		fmt.Printf("MS NIL")
	}
	if ms.clos == nil {
		fmt.Printf("ms.clos nil")
	}
	if ms.clos.L == nil {
		fmt.Printf("ms.clos.L nil")
	}
	ms.clos.L.Lock()
	defer ms.clos.L.Unlock()
	if ms.closed {
		return fmt.Errorf("MultiService already closed, cannot add service!")
	}
	ms.servs = append(ms.servs, s)

	go func() {
		defer func() {
			if x := recover(); x != nil {
				fmt.Printf("run time panic for service: %v\n", x)

				ms.errChan <- fmt.Errorf("Run time panic: %v", x)

			}
		}()
		ms.errChan <- s.Serve()
	}()
	return nil
}

func (ms *MultiService) Serve() error {
	err := <-ms.errChan
	ms.Close()
	ms.WaitClosed()
	return err
}

func (ms *MultiService) Close() error {
	ms.clos.L.Lock()
	defer ms.clos.L.Unlock()
	if ms.closed {
		return nil
	}
	var retErr error = nil
	// close services in reverse order
	for i := len(ms.servs) - 1; i >= 0; i-- {
		s := ms.servs[i]
		err := s.Close()
		if err != nil && retErr == nil {
			retErr = err
		}
		s.WaitClosed()
	}
	ms.closed = true
	ms.clos.Broadcast()
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
