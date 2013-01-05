package maggiefs

import (
	"errors"
)

var (
	E_EXISTS  = errors.New("E_EXISTS")
	E_NOTDIR  = errors.New("E_NOTDIR")
	E_ISDIR   = errors.New("E_ISDIR")
	E_HASDATA = errors.New("E_HASDATA")
	E_NOENT   = errors.New("E_NOENT")
)
