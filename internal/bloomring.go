// Modified from https://github.com/shadowsocks/go-shadowsocks2/blob/master/internal
// Apache-2.0 License

package internal

import (
	"hash/fnv"
)

// simply use Double FNV here as our Bloom Filter hash
func DoubleFNV(b []byte) (uint64, uint64) {
	hx := fnv.New64()
	hx.Write(b)
	x := hx.Sum64()
	hy := fnv.New64a()
	hy.Write(b)
	y := hy.Sum64()
	return x, y
}

type BloomRing struct {
	SlotCapacity int
	SlotPosition int
	SlotCount    int
	EntryCounter int
	Slots        []Filter
}

// NewBloomRing returns a BloomRing without mutex
func NewBloomRing(slot, capacity int, falsePositiveRate float64, h func(b []byte) (uint64, uint64)) *BloomRing {
	// Calculate entries for each slot
	r := &BloomRing{
		SlotCapacity: capacity / slot,
		SlotCount:    slot,
		Slots:        make([]Filter, slot),
	}
	for i := 0; i < slot; i++ {
		r.Slots[i] = NewClassicFilter(r.SlotCapacity, falsePositiveRate, h)
	}
	return r
}

func (r *BloomRing) Add(b []byte) {
	if r == nil {
		return
	}
	slot := r.Slots[r.SlotPosition]
	if r.EntryCounter > r.SlotCapacity {
		// Move to next slot and reset
		r.SlotPosition = (r.SlotPosition + 1) % r.SlotCount
		slot = r.Slots[r.SlotPosition]
		slot.Reset()
		r.EntryCounter = 0
	}
	r.EntryCounter++
	slot.Add(b)
}

func (r *BloomRing) Test(b []byte) bool {
	if r == nil {
		return false
	}
	for _, s := range r.Slots {
		if s.Test(b) {
			return true
		}
	}
	return false
}
