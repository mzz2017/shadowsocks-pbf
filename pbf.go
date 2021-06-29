package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"shadowsocks-pbf/internal"
	"sync"
	"time"
)

func init() {
	gob.RegisterName("internal.Filter", &internal.ClassicFilter{})
}

type AppendFsync int

const (
	AppendFsyncAlways AppendFsync = iota
	AppendFsyncEverySec
	AppendFsyncNo
)

// Those suggest value are all set according to
// https://github.com/shadowsocks/shadowsocks-org/issues/44#issuecomment-281021054
// Due to this package contains various internal implementation so const named with DefaultBR prefix
const (
	DefaultSFCapacity = 1e6
	// FalsePositiveRate
	DefaultSFFPR  = 1e-6
	DefaultSFSlot = 10
)

var (
	SnapshotIsPerformingError = errors.New("another snapshot is performing")
	DamagedSnapshotError      = errors.New("the snapshot is damaged")
)

// File structure:
// | byte length of a single IV (1byte) | byte length of the binary (8byte) | binary of bloomRing | continuous IV without delimiter |

type muFile struct {
	path string
	f    *os.File
	// this mutex guarantee that only one goroutine can operate the file at the same time.
	// however, the order of IV is not necessary
	mu sync.Mutex
}
type snapshot struct {
	busy bool
	mu   sync.Mutex
	buf  bytes.Buffer
}

// PBF is mainly with reference to the mechanism of AOF (Append Only File) of redis.
type PBF struct {
	file        muFile
	lengthIV    uint8
	ring        *internal.BloomRing
	snapshot    snapshot
	appendFsync AppendFsync
	// use this channel to inform the sync goroutine
	closed chan struct{}
}

func New(path string, fsync AppendFsync, lengthIV uint8, slot int, capacity int, falsePositiveRate float64) (*PBF, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	pbf := PBF{
		file:        muFile{f: f, path: path},
		ring:        internal.NewBloomRing(slot, capacity, falsePositiveRate, internal.DoubleFNV),
		appendFsync: fsync,
		lengthIV:    lengthIV,
		closed:      make(chan struct{}),
	}
	if fsync == AppendFsyncEverySec {
		go pbf.syncEverySec()
	}
	return &pbf, nil
}

// NewFromFile will try recovering from file
func NewFromFile(path string, fsync AppendFsync) (*PBF, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	pbf := PBF{
		file:        muFile{f: f, path: path},
		appendFsync: fsync,
		closed:      make(chan struct{}),
	}
	if err = pbf.recoverFromFile(); err != nil {
		return nil, fmt.Errorf("failed to recover from file: %w", err)
	}
	if fsync == AppendFsyncEverySec {
		go pbf.syncEverySec()
	}
	return &pbf, nil
}

func (p *PBF) recoverFromFile() error {
	f2, err := os.OpenFile(p.file.path, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f2.Close()
	var meta [9]byte
	if _, err = f2.Read(meta[:]); err != nil {
		return err
	}
	p.lengthIV = meta[0]
	binaryLength := binary.LittleEndian.Uint64(meta[1:])
	var bloomRingBinary = make([]byte, binaryLength)
	if _, err = f2.Read(bloomRingBinary); err != nil {
		return err
	}
	// recover from binary of bloomRing
	p.ring = new(internal.BloomRing)
	err = gob.NewDecoder(bytes.NewReader(bloomRingBinary)).Decode(p.ring)
	if err != nil {
		return err
	}
	// gob cannot encode and decode functions
	for _, slot := range p.ring.Slots {
		slot.(*internal.ClassicFilter).H = internal.DoubleFNV
	}
	// recover from continuous IV block without delimiter
	for {
		var iv = make([]byte, p.lengthIV)
		n, err := f2.Read(iv)
		if err == io.EOF {
			break
		}
		if n != int(p.lengthIV) {
			return fmt.Errorf("%w: encounter misaligned IV block", DamagedSnapshotError)
		}
		p.ring.Add(iv)
	}
	return nil
}

func (p *PBF) syncEverySec() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		select {
		case <-p.closed:
			ticker.Stop()
			return
		default:
		}
		p.file.mu.Lock()
		_ = p.file.f.Sync()
		p.file.mu.Unlock()
	}
}

func (p *PBF) Close() error {
	select {
	case <-p.closed:
		return nil
	default:
	}
	p.snapshot.mu.Lock()
	defer p.snapshot.mu.Unlock()
	if p.snapshot.busy {
		// wait until not busy
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			if !p.snapshot.busy {
				ticker.Stop()
				break
			}
		}
	}
	close(p.closed)
	p.file.mu.Lock()
	defer p.file.mu.Unlock()
	_ = p.file.f.Sync()
	_ = p.file.f.Close()
	return nil
}

func (p *PBF) Add(b []byte) (err error) {
	p.snapshot.mu.Lock()
	p.ring.Add(b)
	if p.snapshot.busy {
		p.snapshot.buf.Write(b)
	}
	p.snapshot.mu.Unlock()
	// 1. We do not need to guarantee the order of IV in the file.
	// 2. The method of locking in Snapshot and here may cause some writing to the new file, but it is acceptable
	//    because the performance is more important.
	p.file.mu.Lock()
	defer p.file.mu.Unlock()
	if _, err = p.file.f.Write(b); err != nil {
		return
	}
	if p.appendFsync == AppendFsyncAlways {
		if err = p.file.f.Sync(); err != nil {
			return
		}
	}
	return nil
}

func (p *PBF) Test(b []byte) bool {
	p.snapshot.mu.Lock()
	defer p.snapshot.mu.Unlock()
	return p.ring.Test(b)
}

// Snapshot save current binary value to the file.
// It is like the AOF rewrite, but the result of rewrite is binary of bloomRing to save more storage and
// speed up the loading.
func (p *PBF) Snapshot() (err error) {
	// busy status
	p.snapshot.mu.Lock()
	if p.snapshot.busy {
		p.snapshot.mu.Unlock()
		return SnapshotIsPerformingError
	}
	p.snapshot.busy = true
	var ring = *p.ring
	p.snapshot.mu.Unlock()
	defer func() {
		if err != nil {
			//FIXME: how to do if fail
			//p.snapshot.busy = false
		}
	}()
	// this binary may be large and the process may be time-consuming, so it is designed to be concurrent
	bloomBinary := new(bytes.Buffer)
	err = gob.NewEncoder(bloomBinary).Encode(&ring)
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.CreateTemp(filepath.Dir(p.file.path), filepath.Base(p.file.path)+"_*")
	if err != nil {
		panic(err)
	}
	var meta [9]byte
	meta[0] = p.lengthIV
	binary.LittleEndian.PutUint64(meta[1:], uint64(bloomBinary.Len()))
	f.Write(meta[:])
	f.Write(bloomBinary.Bytes())
	// write to a temporary file.
	// to make the buf not grow, we should impose restrictions on Add
	p.snapshot.mu.Lock()
	f.Write(p.snapshot.buf.Bytes())
	p.snapshot.buf.Reset()
	p.snapshot.busy = false
	// we should obtain the file mutex before we allow to Add
	p.file.mu.Lock()
	p.snapshot.mu.Unlock()
	_ = p.file.f.Close()
	p.file.f = f
	f.Sync()
	p.file.mu.Unlock()
	// rename operation does not impact the writing and reading on Unix system, provided that they are in the same file system
	os.Rename(f.Name(), p.file.path)
	return nil
}
