package dataserver

import (
	"net"
	"os"
	"syscall"
)

// uses sendFile to send to a socket
func SendFtoS(in *os.File, out *net.TCPConn, pos int64, length int) (err error) {
	outFile, err := out.File()
	nSent := 0
	for nSent < length {
		n, err := syscall.Sendfile(int(outFile.Fd()), int(in.Fd()), &pos, length)
		if err != nil {
			return err
		}
		if n < length {
			nSent += n
			length -= n
		}
	}
	return nil
}

// splices from in through the provided buffer to out until length is done, using goroutines
func DoSplice(in *os.File, inOff *int64, out *os.File, outOff *int64, p pipe, length int) error {
	inDone := make(chan error)
	outDone := make(chan error)
	inLen := length
	outLen := length

	go func() {
		// splice from in until done
		nSent := 0
		for nSent < inLen {
			nRead, err := syscall.Splice(int(in.Fd()), inOff, int(p.w.Fd()), nil, inLen, 0)
			if err != nil {
				inDone <- err
				return
			}
			if inOff != nil {
				*inOff += nRead
			}
			inLen -= int(nRead)
		}
		inDone <- nil
	}()
	go func() {
		// splice to out until done
		nSent := 0
		for nSent < outLen {
			nRead, err := syscall.Splice(int(p.r.Fd()), nil, int(out.Fd()), outOff, outLen, 0)
			if err != nil {
				outDone <- err
				return
			}
			if outOff != nil {
				*outOff += nRead
			}
			outLen -= int(nRead)
		}
		outDone <- nil
	}()

	errIn := <-inDone
	if errIn != nil {
		return errIn
	}
	errOut := <-outDone
	return errOut
}