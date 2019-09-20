// Copyright 2012 Google, Inc. All rights reserved.
//
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

package reassembly

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/gopacket/layers"
)

var memLog = flag.Bool("assembly_memuse_log", defaultDebug, "If true, the github.com/google/gopacket/reassembly library will log information regarding its memory use every once in a while.")

const pageBlockSize = 1024

type pageBlock struct {
	next, prev *pageBlock
	free       []*page

	// used only in sentinel
	count int
}

/*
 * pageCache
 */
// pageCache is a concurrency-unsafe store of page objects we use to avoid
// memory allocation as much as we can.
type pageCache struct {
	// an array of pageBlock lists, where each bucket stores blocks based on their len(free)
	blockListMap [pageBlockSize + 1]*pageBlock
	minIdx       int // minimum index of bucket which contains blocks
	pcSize       int // number of pageBlocks to allocate
	size, used   int
	pageRequests int64
	ops          int
	nextShrink   int
}

const initialAllocSize = 1024

func newPageCache() *pageCache {
	pc := &pageCache{
		pcSize: 1,
	}

	// initalize list roots
	for i := 0; i <= pageBlockSize; i++ {
		pc.blockListMap[i] = &pageBlock{}
	}

	pc.grow()
	return pc
}

// grow exponentially increases the size of our page cache as much as necessary.
func (c *pageCache) grow() {
	for i := 0; i < c.pcSize; i++ {
		// create a page block with new pages
		block := &pageBlock{}
		block.free = make([]*page, pageBlockSize)
		pages := make([]page, pageBlockSize)
		for j := 0; j < pageBlockSize; j++ {
			pages[j].parent = block
			block.free[j] = &pages[j]
		}

		// put page block to appropriate list
		block.next = c.blockListMap[pageBlockSize].next
		if c.blockListMap[pageBlockSize].next != nil {
			c.blockListMap[pageBlockSize].next.prev = block
		}
		c.blockListMap[pageBlockSize].next = block
		block.prev = c.blockListMap[pageBlockSize]
	}

	c.minIdx = pageBlockSize
	c.size += c.pcSize * pageBlockSize
	c.blockListMap[pageBlockSize].count += c.pcSize

	if *memLog {
		log.Println("PageCache: created", c.pcSize, "new pages, size:", c.size)
	}
	// control next shrink attempt
	c.nextShrink = c.pcSize * pageBlockSize
	c.ops = 0
	// prepare for next alloc
	c.pcSize *= 2
}

// Remove references to unused pages to let GC collect them
// Note: memory used by c.free itself it not collected.
func (c *pageCache) tryShrink() {
	var min = c.pcSize / 2
	if min == 0 {
		min = 1
	}

	// TODO need to take into account non-full blocks
	if c.blockListMap[pageBlockSize].count <= min {
		return
	}

	cur := c.blockListMap[pageBlockSize]
	for i := 0; i < min; i++ {
		cur = cur.next
	}
	cur.next = nil

	freed := c.blockListMap[pageBlockSize].count - min
	c.blockListMap[pageBlockSize].count -= freed
	c.size -= freed * pageBlockSize
	c.pcSize = min
}

// next returns a clean, ready-to-use page object.
func (c *pageCache) next(ts time.Time) (p *page) {
	if *memLog {
		c.pageRequests++
		if c.pageRequests&0xFFFF == 0 {
			log.Println("PageCache:", c.pageRequests, "requested,", c.used, "used")
		}
	}

	idx := c.minIdx

	block := c.blockListMap[idx].next
	if block == nil {
		panic(fmt.Sprintf("memblock error: idx=%v, count=%v", idx, c.blockListMap[idx].count))
	}

	// move block lower
	block.prev.next = block.next
	if block.next != nil {
		block.next.prev = block.prev
	}
	block.next = c.blockListMap[idx-1].next
	if c.blockListMap[idx-1].next != nil {
		c.blockListMap[idx-1].next.prev = block
	}
	c.blockListMap[idx-1].next = block
	block.prev = c.blockListMap[idx-1]
	// update counters
	c.blockListMap[idx].count--
	c.blockListMap[idx-1].count++
	// update minimum index of bucket which contains blocks
	c.minIdx = idx - 1
	if c.minIdx == 0 {
		for i := 1; i < len(c.blockListMap); i++ {
			if c.blockListMap[i].count > 0 {
				c.minIdx = i
				break
			}
		}

		// no free blocks, need to allocate
		if c.minIdx == 0 {
			c.grow()
		}
	}

	i := len(block.free) - 1
	p, block.free = block.free[i], block.free[:i]
	p.seen = ts
	p.bytes = p.buf[:0]
	c.used++
	if *memLog {
		log.Printf("allocator returns %s\n", p)
	}
	c.ops++
	if c.ops > c.nextShrink {
		c.ops = 0
		c.tryShrink()
	}

	return p
}

// replace replaces a page into the pageCache.
func (c *pageCache) replace(p *page) {
	c.used--
	if *memLog {
		log.Printf("replacing %s\n", p)
	}
	block := p.parent
	p.prev = nil
	p.next = nil
	block.free = append(block.free, p)

	i := len(block.free)
	// move block up
	block.prev.next = block.next
	if block.next != nil {
		block.next.prev = block.prev
	}
	block.next = c.blockListMap[i].next
	if c.blockListMap[i].next != nil {
		c.blockListMap[i].next.prev = block
	}
	c.blockListMap[i].next = block
	block.prev = c.blockListMap[i]
	// update counters
	c.blockListMap[i-1].count--
	c.blockListMap[i].count++
	// update minimum index of bucket which contains blocks
	if c.blockListMap[c.minIdx].count == 0 || c.minIdx > i {
		c.minIdx = i
	}
}

/*
 * StreamPool
 */

// StreamPool stores all streams created by Assemblers, allowing multiple
// assemblers to work together on stream processing while enforcing the fact
// that a single stream receives its data serially.  It is safe
// for concurrency, usable by multiple Assemblers at once.
//
// StreamPool handles the creation and storage of Stream objects used by one or
// more Assembler objects.  When a new TCP stream is found by an Assembler, it
// creates an associated Stream by calling its StreamFactory's New method.
// Thereafter (until the stream is closed), that Stream object will receive
// assembled TCP data via Assembler's calls to the stream's Reassembled
// function.
//
// Like the Assembler, StreamPool attempts to minimize allocation.  Unlike the
// Assembler, though, it does have to do some locking to make sure that the
// connection objects it stores are accessible to multiple Assemblers.
type StreamPool struct {
	conns              map[key]*connection
	users              int
	mu                 sync.RWMutex
	factory            StreamFactory
	free               []*connection
	all                [][]connection
	nextAlloc          int
	newConnectionCount int64
}

func (p *StreamPool) grow() {
	conns := make([]connection, p.nextAlloc)
	p.all = append(p.all, conns)
	for i := range conns {
		p.free = append(p.free, &conns[i])
	}
	if *memLog {
		log.Println("StreamPool: created", p.nextAlloc, "new connections")
	}
	p.nextAlloc *= 2
}

// Dump logs all connections
func (p *StreamPool) Dump() {
	p.mu.Lock()
	defer p.mu.Unlock()
	log.Printf("Remaining %d connections: ", len(p.conns))
	for _, conn := range p.conns {
		log.Printf("%v %s", conn.key, conn)
	}
}

func (p *StreamPool) remove(conn *connection) {
	p.mu.Lock()
	if _, ok := p.conns[conn.key]; ok {
		delete(p.conns, conn.key)
		p.free = append(p.free, conn)
	}
	p.mu.Unlock()
}

// NewStreamPool creates a new connection pool.  Streams will
// be created as necessary using the passed-in StreamFactory.
func NewStreamPool(factory StreamFactory) *StreamPool {
	return &StreamPool{
		conns:     make(map[key]*connection, initialAllocSize),
		free:      make([]*connection, 0, initialAllocSize),
		factory:   factory,
		nextAlloc: initialAllocSize,
	}
}

func (p *StreamPool) connections() []*connection {
	p.mu.RLock()
	conns := make([]*connection, 0, len(p.conns))
	for _, conn := range p.conns {
		conns = append(conns, conn)
	}
	p.mu.RUnlock()
	return conns
}

func (p *StreamPool) newConnection(k key, s Stream, ts time.Time) (c *connection, h *halfconnection, r *halfconnection) {
	if *memLog {
		p.newConnectionCount++
		if p.newConnectionCount&0x7FFF == 0 {
			log.Println("StreamPool:", p.newConnectionCount, "requests,", len(p.conns), "used,", len(p.free), "free")
		}
	}
	if len(p.free) == 0 {
		p.grow()
	}
	index := len(p.free) - 1
	c, p.free = p.free[index], p.free[:index]
	c.reset(k, s, ts)
	return c, &c.c2s, &c.s2c
}

func (p *StreamPool) getHalf(k key) (*connection, *halfconnection, *halfconnection) {
	conn := p.conns[k]
	if conn != nil {
		return conn, &conn.c2s, &conn.s2c
	}
	rk := k.Reverse()
	conn = p.conns[rk]
	if conn != nil {
		return conn, &conn.s2c, &conn.c2s
	}
	return nil, nil, nil
}

// getConnection returns a connection.  If end is true and a connection
// does not already exist, returns nil.  This allows us to check for a
// connection without actually creating one if it doesn't already exist.
func (p *StreamPool) getConnection(k key, end bool, ts time.Time, tcp *layers.TCP, ac AssemblerContext) (*connection, *halfconnection, *halfconnection) {
	p.mu.RLock()
	conn, half, rev := p.getHalf(k)
	p.mu.RUnlock()
	if end || conn != nil {
		return conn, half, rev
	}
	s := p.factory.New(k[0], k[1], tcp, ac)
	p.mu.Lock()
	defer p.mu.Unlock()
	conn, half, rev = p.newConnection(k, s, ts)
	conn2, half2, rev2 := p.getHalf(k)
	if conn2 != nil {
		if conn2.key != k {
			panic("FIXME: other dir added in the meantime...")
		}
		// FIXME: delete s ?
		return conn2, half2, rev2
	}
	p.conns[k] = conn
	return conn, half, rev
}
