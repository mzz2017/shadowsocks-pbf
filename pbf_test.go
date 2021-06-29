package main

import (
	"math/rand"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	p, err := New("test.pbf", AppendFsyncEverySec, 32, DefaultSFSlot, DefaultSFCapacity, DefaultSFFPR)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var b [32]byte
	for i := 0; i < 500000; i++ {
		rand.Read(b[:])
		err := p.Add(b[:])
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestNewFromFile(t *testing.T) {
	p, err := NewFromFile("test.pbf", AppendFsyncEverySec)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var b [32]byte
	for i := 0; i < 500000; i++ {
		rand.Read(b[:])
		err := p.Add(b[:])
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPBF_Snapshot(t *testing.T) {
	p, err := New("test.pbf", AppendFsyncEverySec, 32, DefaultSFSlot, DefaultSFCapacity, DefaultSFFPR)
	if err != nil {
		t.Fatal(err)
	}
	var b [32]byte
	var list [][]byte
	go func() {
		time.Sleep(10 * time.Second)
		p.Snapshot()
	}()
	t.Log("data is generating")
	for i := 0; i < 5000000; i++ {
		rand.Read(b[:])
		err := p.Add(b[:])
		if err != nil {
			t.Fatal(err)
		}
		keep := make([]byte, len(b))
		copy(keep, b[:])
		list = append(list, keep)
	}
	t.Log("data has been generated")
	go p.Snapshot()
	p.Close()
	t.Log("test begins")
	p, err = NewFromFile("test.pbf", AppendFsyncEverySec)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(list); i++ {
		if p.Test(list[i]) == false {
			t.Failed()
		}
	}
	p.Close()
}