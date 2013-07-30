package dataserver

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var (
	pipes = make(chan *pipe, 32)
)

const PIPELEN = 1024 * 1024 * 64

//
type pipe struct {
	r           *os.File
	w           *os.File
	numInBuffer int
}

// writes as much as possible to the in version of this pipe
// if would block, returns the number that we did successfully write
func (p *pipe) writeIn(in []byte) (int, error) {
	toWrite := len(in)
	if p.numInBuffer+toWrite > PIPELEN {
		toWrite = PIPELEN - p.numInBuffer
		in = in[:toWrite]
	}
	nWritten := 0
	for nWritten < toWrite {
		w, err := p.w.Write(in[nWritten : toWrite-1])
		nWritten += w
		if err != nil {
			return nWritten, err
		}
	}
	return nWritten, nil
}

func (p *pipe) spliceIn(in *os.File, inOff *int64, length int) (int64, error) {
	if p.numInBuffer+length > PIPELEN {
		length = PIPELEN - p.numInBuffer
	}
	spliced := int64(0)
	for spliced < int64(length) {
		w, err := syscall.Splice(int(in.Fd()), inOff, int(p.w.Fd()), nil, length, 0)
		if err != nil {
			return spliced, err
		}
		spliced += w
		if inOff != nil {
			*inOff += w
		}
	}
	return spliced, nil
}

// tees in (duplicates) all data from the other pipe to this one
func (p *pipe) teeIn(teeFrom *pipe) error {
	toTee := teeFrom.numInBuffer
	for toTee > 0 {
		// no offset to tee arguments for now
		n, err := syscall.Tee(int(teeFrom.r.Fd()), int(p.w.Fd()), toTee, 0)
		if err != nil {
			return err
		}
		toTee -= int(n)
	}
	return nil
}

// splices all contents out to the specified file, updates outOff
func (p *pipe) spliceOut(out *os.File, outOff *int64) error {
	toSplice := p.numInBuffer
	for toSplice > 0 {
		n, err := syscall.Splice(int(p.r.Fd()), nil, int(out.Fd()), outOff, toSplice, 0)
		if err != nil {
			return err
		}
		toSplice -= int(n)
		if outOff != nil {
			*outOff -= n
		}
	}
	p.numInBuffer = 0
	return nil
}

func (p pipe) Close() {
	_ = p.r.Close()
	_ = p.w.Close()
}

// either gets a pipe off the pool or returns a brand new one
func initPipe() (p *pipe, err error) {
	var pp [2]int32
	_, _, e := syscall.RawSyscall(syscall.SYS_PIPE2, uintptr(unsafe.Pointer(&pp)), syscall.O_CLOEXEC, 0)
	if e != 0 {
		return nil, os.NewSyscallError("pipe", e)
	}
	fmt.Printf("Got new pipe, read fd %d, write fd %d\n",pp[0],pp[1])
	return &pipe{os.NewFile(uintptr(pp[0]), "|0"), os.NewFile(uintptr(pp[1]), "|1"), 0}, nil

}

func getPipe() (p *pipe, err error) {
	select {
	case p = <-pipes:
	// got one off pool
	default:
		// none free, so allocate
		p, err = initPipe()
	}
	return p, err
}

func returnPipe(p *pipe) {
	// return pipe
	select {
	case pipes <- p:
		// returned to pool
	default:
		// pool full, close this guy
		p.Close()
	}
}

// uses internal pipe buffer to splice bytes from in to out
func Splice(in *os.File, inOff *int64, out *os.File, outOff *int64, length int) (int, error) {
	nWritten := 0
	p, err := getPipe()
	defer p.Close()
	if err != nil {
		return nWritten, err
	}
	for length > 0 {
		r, err := p.spliceIn(in, inOff, length)
		if err != nil {
			return nWritten, err
		}
		err = p.spliceOut(out, outOff)
		*outOff += r
		length -= int(r)
		nWritten += int(r)
	}

	return nWritten, err
}

// uses an internal pipe to splice from in to off.  splices all before returning.
// header, if not nil, is pre-pended to the spliced bytes.
func SpliceWithHeader(in *os.File, inOff *int64, out *os.File, outOff *int64, length int, header []byte) (int, error) {
	nWritten := 0
	p, err := getPipe()
	defer p.Close()
	if err != nil {
		return nWritten, err
	}
	if header != nil {
		// splice header into pipe
		headerWritten, err := p.writeIn(header)
		if err != nil {
			return 0, err
		}
		// very rare case that header was bigger than pipe size, splice it all through
		for headerWritten < len(header) {
			p.spliceOut(out, outOff)
			w, err := p.writeIn(header[headerWritten:])
			if err != nil {
				return headerWritten, err
			}
			headerWritten += w
		}
		nWritten += headerWritten

	}
	// now do actual file
	for length > 0 {
		r, err := p.spliceIn(in, inOff, length)
		if err != nil {
			return nWritten, err
		}
		err = p.spliceOut(out, outOff)
		*outOff += r
		length -= int(r)
		nWritten += int(r)
	}

	return nWritten, err
}

func SpliceWithTee(in *os.File, inOff *int64, out *os.File, outOff *int64, teeOut *os.File, length int) error {
	p, err := getPipe()
	if err != nil {
		return err
	}
	defer p.Close()
	teePipe, err := getPipe()
	if err != nil {
		return err
	}
	defer teePipe.Close()
	for length > 0 {
		// splice into buffer
		chunkSize, err := p.spliceIn(in, inOff, length)
		fmt.Printf("Spliced %d into pipe\n", chunkSize)
		if err != nil {
			return fmt.Errorf("Error splicing into pipe fd %d from input fd %d : %s", p.w.Fd(), in.Fd(), err.Error())
		}
		// tee to other buffer
		err = teePipe.teeIn(p)
		if err != nil {
			return fmt.Errorf("Error teeing from pipe to pipe fd %d : %s", teePipe.w.Fd(), err.Error())
		}

		// splice out to tee dest
		err = teePipe.spliceOut(teeOut, nil)
		if err != nil {
			return fmt.Errorf("Error splicing from pipe to fd %d : %s", out.Fd(), err.Error())
		}
		// splice out to out dest
		err = p.spliceOut(out, outOff)
		if err != nil {
			return fmt.Errorf("Error splicing from pipe to fd %d : %s", out.Fd(), err.Error())
		}
		length -= int(chunkSize)
	}
	return nil

}

// splices from in to out, while teeing to all teeFiles.  uses pipe from internal buffer pool.  does not return until all bytes transferred (or error).
//func SpliceAdv(in *os.File, inOff *int64, out *os.File, outOff *int64, teeFiles []*os.File, length int) error {
//	return withPipe(func(p *pipe) error {
//		totalSpliced := 0
//		for length > 0 {
//			inBuff, err := syscall.Splice(int(in.Fd()), inOff, int(p.w.Fd()), nil, length, 0)
//			if err != nil {
//				return err
//			}
//			length -= int(inBuff)
//			if inOff != nil {
//				*inOff -= int64(inBuff)
//			}
//			// do tee
//			if teeFiles != nil && len(teeFiles) > 0 {
//				for _, tee := range teeFiles {
//					// make sure we tee splFrom
//					toTee := totalSpliced
//					for toTee > 0 {
//						// no offset to tee arguments for now
//						n, err := syscall.Tee(int(p.r.Fd()), int(tee.Fd()), toTee, 0)
//						if err != nil {
//							return err
//						}
//						toTee -= int(n)
//					}
//				}
//			}
//			// do splice
//			toSplice := totalSpliced
//			for toSplice > 0 {
//				n, err := syscall.Splice(int(p.r.Fd()), nil, int(out.Fd()), outOff, toSplice, 0)
//				if err != nil {
//					return err
//				}
//				toSplice -= int(n)
//				*outOff -= n
//			}
//		}
//		return nil
//	})
//}
