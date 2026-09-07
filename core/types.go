package vadb

import (
	"fmt"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"
)

type IVaDB struct {
	cli  valkey.Client
	setg *ISettings
	mu   sync.Mutex
	rmu  sync.RWMutex
	wg   sync.WaitGroup
}
type IConfig struct {
	Set *ISettings
	Cli valkey.Client
}

func New(c IConfig) *IVaDB {
	return &IVaDB{
		cli:  c.Cli,
		setg: c.Set,
	}
}

const (
	guard = "::"
)

type DType string

const (
	MAP DType = "MAP"
	DLL DType = "DLL"
	SET DType = "SET"
)

var (
	errValidMode error = fmt.Errorf("mode not match")
)

type IPush struct {
	Bucket, Branch, Object string // specify the name
	Data                   string // data to push
	err                    error
}

type ISettings struct {
	TTL time.Duration
}
