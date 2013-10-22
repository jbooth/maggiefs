package dataserver

import (
	"fmt"
	"io"
	"net"
	"syscall"
)

// wraps ReadWriteCloser with Stringer for debugging info
type Endpoint interface {
	io.ReadWriteCloser
	fmt.Stringer
}

type endpt struct {
	r      io.Reader
	w      io.Writer
	desc   string
	sameFD bool // whether they point to the same fd or not
}

func (e *endpt) Read(p []byte) (n int, err error)  { return e.r.Read(p) }
func (e *endpt) Write(p []byte) (n int, err error) { return e.w.Write(p) }
func (e *endpt) String() string                    { return e.desc }
func (e *endpt) Close() error {
	closer, ok := e.r.(io.Closer)
	var err error
	if ok {
		err = closer.Close()
	}
	if !e.sameFD {
		closer, ok = e.w.(io.Closer)
		if ok {
			err = closer.Close()
		}
	}
	return err
}

// wrap a socket with nice desc
func SockEndpoint(c *net.TCPConn) (Endpoint) {
	desc := fmt.Sprintf("Socket(nonblock): local %s to remote %s", c.LocalAddr(), c.RemoteAddr())
	c.SetNoDelay(true)
	c.SetReadBuffer(128 * 1024)
	return &endpt{c, c, desc, true}
}

func BlockingSockEndPoint(c *net.TCPConn) (Endpoint, error) {
	desc := fmt.Sprintf("Socket(blocking): local %s to remote %s", c.LocalAddr(), c.RemoteAddr())
	err := c.SetNoDelay(true)
	if err != nil { return nil,err }
	err = c.SetReadBuffer(128 * 1024)
	if err != nil { return nil,err }
	f, err := c.File()
	defer c.Close()
	if err != nil {
		return nil, err
	}
	syscall.SetNonblock(int(f.Fd()), false)

	return &endpt{f, f, desc, true}, nil
}

// matching pipe endpoints
func PipeEndpoints() (Endpoint, Endpoint) {
	leftRead, rightWrite := io.Pipe()
	rightRead, leftWrite := io.Pipe()
	left := &endpt{leftRead, leftWrite, "Pipe", false}
	right := &endpt{rightRead, rightWrite, "Pipe", false}
	return left, right
}

var buffPool = make(chan []byte, 32)

func getBuff() []byte {
	var b []byte
	select {
	case b = <-buffPool:
	// got one off pool
	default:
		// none free, so allocate
		b = make([]byte, 1024*64)
	}
	return b
}

func returnBuff(b []byte) {
	// return pipe
	select {
	case buffPool <- b:
		// returned to pool
	default:
		// pool full, GC will handle
	}
}

func Copy(dst io.Writer, src io.Reader, n int64) (int64, error) {
	// If the writer has a ReadFrom method, use it to do the copy.
	// Avoids an allocation and a copy.
	if rt, ok := dst.(io.ReaderFrom); ok {
		return rt.ReadFrom(src)
	}
	// Similarly, if the reader has a WriteTo method, use it to do the copy.
	if wt, ok := src.(io.WriterTo); ok {
		return wt.WriteTo(dst)
	}

	buff := getBuff()
	defer returnBuff(buff)
	nWritten := int64(0)
	for nWritten < n {
		r, err := src.Read(buff)
		if r > 0 {
			w, e2 := dst.Write(buff[0:r])
			if w > 0 {
				nWritten += int64(w)
			}
			if e2 == io.EOF {
				fmt.Printf("Copy: EOF, read %d write %d nWritten %d out of %d\n", r, w, nWritten, n)

			}
			if e2 != nil && e2 != io.EOF {
				return nWritten, e2
			}
			if r != w {
				fmt.Printf("Copy: ErrShortWrite, read %d write %d nWritten %d out of %d\n", r, w, nWritten, n)
				return nWritten, io.ErrShortWrite
			}
		}
		if err == io.EOF {
			return nWritten, nil
		}
		if err != nil {
			return nWritten, err
		}
	}
	return nWritten, nil
}

func NewSectionWriter(w io.WriterAt, off int64, length int64) io.Writer {
	return &SectionWriter{w, off, off + length}
}

// SectionReader implements Write on a section
// of an underlying WriteAt.
type SectionWriter struct {
	w     io.WriterAt
	off   int64
	limit int64
}

func (s *SectionWriter) Write(p []byte) (n int, err error) {
	if s.off >= s.limit {
		return 0, io.EOF
	}
	if max := s.limit - s.off; int64(len(p)) > max {
		p = p[0:max]
	}
	n, err = s.w.WriteAt(p, s.off)
	s.off += int64(n)
	return
}
