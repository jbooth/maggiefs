package maggiefs

type OpenFile struct {

}

// io.Seeker
func (f *OpenFile) Seek(offset int64, whence int) (ret int64, err error) {
  return int64(0),nil
}

// io.Reader
func (f *OpenFile) Read(p []byte) (n int, err error) {
  return int(0),nil
}

//io.Writer
func (f *OpenFile) Write(p []byte) (n int, err error) {
  return int(0),nil
}

//io.Closer
func (f *OpenFile) Close() error {
  return nil
}


