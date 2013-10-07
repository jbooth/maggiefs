package mrpc


type Service interface {
	// serves clients, blocking until we either crash or are told to stop
	Serve() error
	// requests stop
	Close() error
	// waits till actually stopped
	WaitClosed() error
}