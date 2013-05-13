package dataserver

import (
	"fmt"
	"io"
	"os"
	"syscall"
	"unsafe"
)

var (
	pipes = make(chan pipe, 16)
)

type pipe struct {
	r *os.File
	w *os.File
}

func (p pipe) Close() {
	_ = p.r.Close()
	_ = p.w.Close()
}

// either gets a pipe off the pool or returns a brand new one
func initPipe() (p pipe, err error) {
	var pp [2]int32
	_, _, e := syscall.RawSyscall(syscall.SYS_PIPE2, uintptr(unsafe.Pointer(&pp)), syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if e != 0 {
		return pipe{}, os.NewSyscallError("pipe", e)
	}
	return pipe{os.NewFile(uintptr(pp[0]), "|0"), os.NewFile(uintptr(pp[1]), "|1")}, nil

}

func withPipe(f func(p pipe) error) (err error) {
	var p pipe
	select {
	case p = <-pipes:
	// got one off pool
	default:
		// none free, so allocate
		p, err = initPipe()
	}
	if err != nil {
		return err
	}

	err = f(p)
	if err != nil {
		// make a new pipe on any error, as old one could have garbage in it
		var initErr error
		p, initErr = initPipe()
		if initErr != nil {
			return initErr
		}
	}

	// return pipe
	select {
	case pipes <- p:
		// returned to pool
	default:
		// pool full, close this guy
		p.Close()
	}
	return err
}

// uses sendFile to send to a socket
func SendFile(in *os.File, outFile *os.File, pos int64, length int) (err error) {
	nSent := 0
	buff := make([]byte, 4096)
	outputFirstFive := true
	for nSent < length {
		numTransfer := 4096
		if length-nSent < 4096 {
			numTransfer = length - nSent
			buff = buff[0:numTransfer]
		}
		_, err = in.ReadAt(buff,pos)
		if err != nil {
			return fmt.Errorf("SendFile: Error reading from in file : %s", err.Error())
		}
		if outputFirstFive {
			outputFirstFive = false
			fmt.Printf("SendFile first 5 %x\n",buff[:5])
		}
		_, err = outFile.Write(buff)
		if err != nil {
			return fmt.Errorf("SendFile:  Error writing to splice-out file : %s", err.Error())
		}
		nSent += numTransfer
		pos += int64(numTransfer)
		// TODO actually use sendfile
		//		fmt.Println("calling sendfile")
		//		n, err := syscall.Sendfile(int(outFile.Fd()), int(in.Fd()), &pos, length)
		//		fmt.Printf("sent %d bytes, total: %d\n",n,nSent+n)
		//		if err != nil {
		//			return err
		//		}
		//		if n < length {
		//			nSent += n
		//			length -= n
		//		}
	}
	return nil
}

// temp implementation that just uses a buffer
func SpliceAdv(in *os.File, inOff *int64, out *os.File, outOff *int64, teeFiles []*os.File, length int) error {
	buff := make([]byte, 4096)
	nSpliced := 0
	var err error
	outputOffset := int64(0)
	outputFirstFive := true
	if outOff != nil {
		outputOffset = *outOff
	}
	for nSpliced < length {
		numTransfer := 4096
		if length-nSpliced < 4096 {
			numTransfer = length - nSpliced
			buff = buff[0:numTransfer]
		}
		_, err = io.ReadFull(in, buff)
		if err != nil {
			return fmt.Errorf("SpliceAdv: Error reading from in file : %s", err.Error())
		}
		if outputFirstFive {
			outputFirstFive = false
			fmt.Printf("SpliceAdv first 5 %x\n",buff[:5])
		}
		_, err = out.WriteAt(buff, outputOffset)
		if err != nil {
			return fmt.Errorf("SpliceAdv:  Error writing to splice-out file : %s", err.Error())
		}
		if teeFiles != nil {
			for _, o := range teeFiles {
				_, err = o.Write(buff)
				if err != nil {
					return fmt.Errorf("SpliceAdv:  Error tee-ing : %s", err.Error())
				}
			}
			nSpliced += numTransfer
			outputOffset += int64(numTransfer)
		}
	}
	return nil
}

// commented out for now

//// splices from in to out, while teeing to all teeFiles.  uses pipe from internal buffer pool.  does not return until all bytes transferred (or error).
//func SpliceAdv(in *os.File, inOff *int64, out *os.File, outOff *int64, teeFiles []*os.File, length int) error {
//	return withPipe(func(p pipe) error {
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
