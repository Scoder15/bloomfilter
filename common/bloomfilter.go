package common

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

type BloomFilterIf interface {
	// Insert an element into the set.
	Add(e []byte)

	// Determine if an element is in the set
	Check(x []byte) bool

	//持久化到硬盘
	Dump(file string) error

	//从硬盘载入
	Load(file string) error
}

// The standard bloom filter, which allows adding of
// elements, and checking for their existence
type BloomFilter struct {
	bitmap []bool      // The bloom-filter bitmap
	k      int         // Number of hash functions
	n      int         // Number of elements in the filter
	m      int         // Size of the bloom filter
	hashfn hash.Hash64 // The hash function
}

// Returns a new BloomFilter object, if you pass the
// number of Hash Functions to use and the maximum
// size of the Bloom Filter
func NewBloomFilter(numHashFuncs, bfSize int) *BloomFilter {
	bf := new(BloomFilter)
	bf.bitmap = make([]bool, bfSize)
	bf.k, bf.m = numHashFuncs, bfSize
	bf.n = 0
	bf.hashfn = fnv.New64()
	return bf
}

func (bf *BloomFilter) getHash(b []byte) (uint32, uint32) {
	bf.hashfn.Reset()
	bf.hashfn.Write(b)
	hash64 := bf.hashfn.Sum64()
	h1 := uint32(hash64 & ((1 << 32) - 1))
	h2 := uint32(hash64 >> 32)
	return h1, h2
}

// Adds an element (in byte-array form) to the Bloom Filter
func (bf *BloomFilter) Add(e []byte) {
	h1, h2 := bf.getHash(e)
	for i := 0; i < bf.k; i++ {
		ind := (h1 + uint32(i)*h2) % uint32(bf.m)
		bf.bitmap[ind] = true
	}
	bf.n++
}

// Checks if an element (in byte-array form) exists in the
// Bloom Filter
func (bf *BloomFilter) Check(x []byte) bool {
	h1, h2 := bf.getHash(x)
	result := true
	for i := 0; i < bf.k; i++ {
		ind := (h1 + uint32(i)*h2) % uint32(bf.m)
		result = result && bf.bitmap[ind]
	}
	return result
}

// Returns the current False Positive Rate of the Bloom Filter
func (bf *BloomFilter) FalsePositiveRate() float64 {
	return math.Pow((1 - math.Exp(-float64(bf.k*bf.n)/
		float64(bf.m))), float64(bf.k))
}

// A Bloom Filter which allows deletion of elements.
// An 8-bit counter is maintained for each slot. This should
// be accounted for while deciding the size of the new filter.
type CountingBloomFilter struct {
	counts []uint8     // The bloom-filter bitmap
	k      int         // Number of hash functions
	n      int         // Number of elements in the filter
	m      int         // Size of the bloom filter
	hashfn hash.Hash64 // The hash function
}

// Creates a new Counting Bloom Filter
func NewCountingBloomFilter(numHashFuncs, cbfSize int) *CountingBloomFilter {
	cbf := new(CountingBloomFilter)
	cbf.counts = make([]uint8, cbfSize)
	cbf.k, cbf.m = numHashFuncs, cbfSize
	cbf.n = 0
	cbf.hashfn = fnv.New64()
	return cbf
}

func (cbf *CountingBloomFilter) getHash(b []byte) (uint32, uint32) {
	cbf.hashfn.Reset()
	cbf.hashfn.Write(b)
	hash64 := cbf.hashfn.Sum64()
	h1 := uint32(hash64 & ((1 << 32) - 1))
	h2 := uint32(hash64 >> 32)
	return h1, h2
}

// Adds an element (in byte-array form) to the Counting Bloom Filter
func (cbf *CountingBloomFilter) Add(e []byte) {
	h1, h2 := cbf.getHash(e)
	for i := 0; i < cbf.k; i++ {
		ind := (h1 + uint32(i)*h2) % uint32(cbf.m)
		// Guarding against an overflow
		if cbf.counts[ind] < 0xFF {
			cbf.counts[ind] += 1
		}
	}
	cbf.n++
}

// Removes an element (in byte-array form) from the Counting Bloom Filter
func (cbf *CountingBloomFilter) Remove(e []byte) {
	h1, h2 := cbf.getHash(e)
	for i := 0; i < cbf.k; i++ {
		ind := (h1 + uint32(i)*h2) % uint32(cbf.m)

		if cbf.counts[ind] > 0 {
			// Guarding against an underflow
			cbf.counts[ind] -= 1
		}
	}
	cbf.n--
}

// Checks if an element (in byte-array form) exists in the
// Counting Bloom Filter
func (cbf *CountingBloomFilter) Check(x []byte) bool {
	h1, h2 := cbf.getHash(x)
	result := true
	for i := 0; i < cbf.k; i++ {
		ind := (h1 + uint32(i)*h2) % uint32(cbf.m)
		result = result && (cbf.counts[ind] > 0)
	}
	return result
}

// A scalable bloom filter, which allows adding of
// elements, and checking for their existence
type ScalableBloomFilter struct {
	bfArr []BloomFilter // The list of Bloom Filters
	k     int           // Number of hash functions
	n     int           // Number of elements in the filter
	m     int           // Size of the smallest bloom filter
	p     int           // Maximum number of bloom filters to support.
	q     int           // Number of bloom filters present in the list.
	r     int           // Multiplication factor for new bloom filter sizes
	s     int           // Size of the current bloom filter
	f     float64       // Target False Positive rate / bf
}

// Returns a new Scalable BloomFilter object, if you pass in
// valid values for all the required fields.
// firstBFSize is the size of the first Bloom Filter which
// will be created.
// maxBloomFilters is the upper limit on the number of
// Bloom Filters to create
// growthFactor is the rate at which the Bloom Filter size grows.
// targetFPR is the maximum false positive rate allowed for each
// of the constituent bloom filters, after which a new Bloom
// Filter would be created and used
func NewScalableBloomFilter(numHashFuncs, firstBFSize, maxBloomFilters, growthFactor int, targetFPR float64) *ScalableBloomFilter {
	sbf := new(ScalableBloomFilter)
	sbf.k, sbf.n, sbf.m, sbf.p, sbf.q, sbf.r, sbf.f = numHashFuncs, 0, firstBFSize, maxBloomFilters, 1, growthFactor, targetFPR
	sbf.s = sbf.m
	sbf.bfArr = make([]BloomFilter, 0, maxBloomFilters)
	bf := NewBloomFilter(sbf.k, sbf.m)
	sbf.bfArr = append(sbf.bfArr, *bf)
	return sbf
}

// Adds an element of type byte-array to the Bloom Filter
func (sbf *ScalableBloomFilter) Add(e []byte) {
	inuseFilter := sbf.q - 1
	fpr := sbf.bfArr[inuseFilter].FalsePositiveRate()
	if fpr <= sbf.f {
		sbf.bfArr[inuseFilter].Add(e)
		sbf.n++
	} else {
		if sbf.p == sbf.q {
			return
		}
		sbf.s = sbf.s * sbf.r
		bf := NewBloomFilter(sbf.k, sbf.s)
		sbf.bfArr = append(sbf.bfArr, *bf)
		sbf.q++
		inuseFilter = sbf.q - 1
		sbf.bfArr[inuseFilter].Add(e)
		sbf.n++
	}
}

// Returns the cumulative False Positive Rate of the filter
func (sbf *ScalableBloomFilter) FalsePositiveRate() float64 {
	res := 1.0
	for i := 0; i < sbf.q; i++ {
		res *= (1.0 - sbf.bfArr[i].FalsePositiveRate())
	}
	return 1.0 - res
}

// Checks if an element (in byte-array form) exists
func (sbf *ScalableBloomFilter) Check(e []byte) bool {
	for i := 0; i < sbf.q; i++ {
		if sbf.bfArr[i].Check(e) {
			return true
		}
	}
	return false
}

// 基于天级的bloomfilter
type DailyBloomFilter struct {
	bms         []*BloomFilter
	bfAlignSize int          // 按多少对齐
	bfNum       int          // 保存多少天
	periodPerBf int64        // 每个bloomfilter保存多少秒
	k           int          // Number of hash functions
	m           int          // Size of the bloom filter
	curIdx      int          // 当前指向bms的idx
	lastTime    int64        //上次bloomfilter启动时间
	bfDelayFree *BloomFilter //用于延迟是否的bloomfilter
	hashfn      hash.Hash64
	mylock      sync.Mutex
	needDump    []int
}

// Returns a new BloomFilter object, if you pass the
// number of Hash Functions to use and the maximum
// size of the Bloom Filter
func NewDailyBloomFilter(howManyBf, numHashFuncs, bfSize int, period int64) *DailyBloomFilter {
	if howManyBf <= 0 || numHashFuncs <= 0 || bfSize <= 0 || period <= 0 {
		return nil
	}

	bf := new(DailyBloomFilter)
	bf.needDump = []int{}
	bf.bfAlignSize = 8 * 1024 * 128
	bf.bfNum = howManyBf
	bf.bms = make([]*BloomFilter, bf.bfNum)
	bf.k = numHashFuncs
	bf.m = ((bfSize-1)/bf.bfAlignSize + 1) * bf.bfAlignSize
	bf.periodPerBf = period
	bf.curIdx = -1
	bf.hashfn = fnv.New64()
	bf.lastTime = time.Now().Unix()
	return bf
}

// Adds an element (in byte-array form) to the Bloom Filter
func (bf *DailyBloomFilter) Add(e []byte) {
	h1, h2 := bf.getHash(e)
	if bf.curIdx == -1 {
		bf.mylock.Lock()
		//加锁后再次检查
		if bf.curIdx == -1 {
			bf.bms[0] = NewBloomFilter(bf.k, bf.m)
			bf.curIdx = 0
			bf.lastTime = time.Now().Unix()
		}
		bf.dumpAdd(0)
		bf.mylock.Unlock()
	} else if h1%10000 == 0 {
		//每10000次做一次检查，check时间是否要切到新的bloomfilter
		if time.Now().Unix()-bf.lastTime > bf.periodPerBf {
			bf.mylock.Lock()
			// 时间长了就切到新的bloomfilter上
			if time.Now().Unix()-bf.lastTime > bf.periodPerBf {
				bf.lastTime = time.Now().Unix()
				nextIdx := (bf.curIdx + 1) % bf.bfNum
				if bf.bms[nextIdx] != nil {
					// 放到延迟释放bf上
					bf.bfDelayFree = bf.bms[nextIdx]
					bf.bms[nextIdx] = nil
				}
				bf.bms[nextIdx] = NewBloomFilter(bf.k, bf.m)
				bf.dumpAdd(bf.curIdx)
				bf.dumpAdd(nextIdx)
				bf.curIdx = nextIdx
			}
			bf.mylock.Unlock()
		}
	}
	for i := 0; i < bf.k; i++ {
		ind := (h1 + uint32(i)*h2) % uint32(bf.bms[bf.curIdx].m)
		bf.bms[bf.curIdx].bitmap[ind] = true
	}
}

// Check checks if an element (in byte-array form) exists in the
// Bloom Filter
func (bf *DailyBloomFilter) Check(x []byte) bool {
	h1, h2 := bf.getHash(x)
	for j := 0; j < bf.bfNum; j++ {
		if bf.bms[j] == nil {
			continue
		}
		result := true
		for i := 0; i < bf.bms[j].k; i++ {
			ind := (h1 + uint32(i)*h2) % uint32(bf.bms[j].m)
			result = result && bf.bms[j].bitmap[ind]
		}
		if result == true {
			return true
		}
	}
	return false
}

// CheckRecentN checks if an element (in byte-array form) exists in the
// recent n Bloom Filter
func (bf *DailyBloomFilter) CheckRecentN(x []byte, n int) bool {
	if n <= 0 || n >= bf.bfNum {
		return bf.Check(x)
	}
	curIdx := bf.curIdx
	h1, h2 := bf.getHash(x)
	for k := 0; k < n; k++ {
		j := (curIdx - k + bf.bfNum) % bf.bfNum
		if bf.bms[j] == nil {
			continue
		}
		result := true
		for i := 0; i < bf.bms[j].k; i++ {
			ind := (h1 + uint32(i)*h2) % uint32(bf.bms[j].m)
			result = result && bf.bms[j].bitmap[ind]
		}
		if result == true {
			return true
		}
	}
	return false
}

//持久化到硬盘
// file.1: numHashFuncs, bfSize,timestamp,binary
func (bf *DailyBloomFilter) Dump(file string) error {
	if bf.curIdx == -1 {
		return nil
	}
	var buf *bytes.Buffer
	var myfile string
	var mydump []int
	var err error

	// 把需要dump的id拷贝出来，如果没有完成，还要塞回去
	bf.mylock.Lock()
	mydump = bf.needDump
	bf.needDump = nil
	bf.mylock.Unlock()

	for len(mydump) > 0 {
		ele := mydump[0]
		buf = bytes.NewBuffer([]byte{})
		buf.WriteString(file)
		buf.WriteString(strconv.Itoa(ele))
		myfile = buf.String()
		isexist := false
		_, err = os.Stat(myfile)
		if err == nil {
			isexist = true
		} else if os.IsNotExist(err) {
			isexist = false
			// file does not exist
		} else {
			break
		}
		var mybakflie string
		if isexist {
			mybakflie = myfile + ".bak"
			err = os.Rename(myfile, mybakflie)
			if err != nil {
				break
			}
		}
		err = bf.dumpFile(ele, myfile)
		if err != nil {
			break
		}
		mydump = mydump[1:]
		os.Remove(mybakflie)
	}

	bf.mylock.Lock()
	mydump = append(mydump, bf.curIdx)
	for i := 0; i < len(mydump); i++ {
		bf.dumpAdd(mydump[i])
	}
	bf.mylock.Unlock()

	return err
}

//从硬盘载入
func (bf *DailyBloomFilter) Load(file string) error {
	var mynewtime int64
	curidx := -1
	mynewtime = 0
	for i := 0; i < bf.bfNum; i++ {
		buf := bytes.NewBuffer([]byte{})
		buf.WriteString(file)
		buf.WriteString(strconv.Itoa(i))
		myfile := buf.String()
		fi, err1 := os.Stat(myfile)
		if err1 != nil {
			if os.IsNotExist(err1) {
				continue
			} else {
				return err1
			}
		}
		filesize := fi.Size()
		f, err := os.Open(myfile)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			} else {
				return err
			}
		}
		defer f.Close()

		var k, m int32
		var mytime int64
		headsize := binary.Size(k) + binary.Size(m) + binary.Size(mytime)
		b := make([]byte, headsize)
		n, err := f.Read(b)
		if err != nil {
			return err
		} else if n != headsize {
			return fmt.Errorf("%s head error", myfile)
		}
		byteBuf := bytes.NewBuffer(b)

		err = binary.Read(byteBuf, binary.BigEndian, &k)
		if err != nil {
			return err
		}

		err = binary.Read(byteBuf, binary.BigEndian, &m)
		if err != nil {
			return err
		}

		err = binary.Read(byteBuf, binary.BigEndian, &mytime)
		if err != nil {
			return err
		}
		if mytime > mynewtime {
			mynewtime = mytime
			curidx = i
			bf.lastTime = mynewtime
		} else if mytime == mynewtime {
			if curidx+1 == i {
				curidx = i
			}
		}
		bf.bms[i] = NewBloomFilter(int(k), int(m))
		if int32(filesize)-int32(headsize) == m {
			b = make([]byte, bf.bfAlignSize)
			boolarray := make([]bool, bf.bfAlignSize)
			for j := 0; j < int(m); j += bf.bfAlignSize {
				n, err = f.Read(b)
				if err != nil {
					return err
				} else if n != bf.bfAlignSize {
					return fmt.Errorf("%s read content %d error", myfile, j)
				}

				byteBuf = bytes.NewBuffer(b)
				err = binary.Read(byteBuf, binary.BigEndian, boolarray)
				if err != nil {
					return fmt.Errorf("%s unserialize content %d error", myfile, j)
				}
				copy(bf.bms[i].bitmap[j:], boolarray)
			}
		} else if 8*(int32(filesize)-int32(headsize)) == m {
			//将一个字节解成8个bool
			var a uint8
			b = make([]byte, bf.bfAlignSize/8)
			boolarray := make([]bool, bf.bfAlignSize)
			for j := 0; j < int(m/8); j += bf.bfAlignSize / 8 {
				n, err = f.Read(b)
				if err != nil {
					return err
				} else if n != bf.bfAlignSize/8 {
					return fmt.Errorf("%s read content %d error", myfile, j)
				}

				byteBuf = bytes.NewBuffer(b)
				for k := 0; k < bf.bfAlignSize/8; k++ {
					err = binary.Read(byteBuf, binary.BigEndian, &a)
					if err != nil {
						return fmt.Errorf("%s unserialize content %d,%d error", myfile, j, k)
					}
					for m := 7; m >= 0; m-- {
						c := a % 2
						if c == 0 {
							boolarray[8*k+m] = false
						} else {
							boolarray[8*k+m] = true
						}
						a = a / 2
					}
				}

				copy(bf.bms[i].bitmap[j*8:], boolarray)
			}
		} else {
			fmt.Printf("error file %s,size %d\n", myfile, filesize)
			continue
			// return fmt.Errorf("error file %s,size %d", myfile, filesize)
		}
	}
	bf.curIdx = curidx
	return nil
}

func (bf *DailyBloomFilter) getHash(b []byte) (uint32, uint32) {
	bf.hashfn.Reset()
	bf.hashfn.Write(b)
	hash64 := bf.hashfn.Sum64()
	h1 := uint32(hash64 & ((1 << 32) - 1))
	h2 := uint32(hash64 >> 32)
	return h1, h2
}

// 做一个去重判断
func (bf *DailyBloomFilter) dumpAdd(idx int) {
	i := len(bf.needDump)
	needadd := true
	i = i - 1
	for i >= 0 {
		if bf.needDump[i] == idx {
			needadd = false
			break
		} else {
			i = i - 1
		}
	}
	if needadd {
		bf.needDump = append(bf.needDump, idx)
	}
}

// dump第idx个bloomfilter到一个具体文件
// file.1: numHashFuncs, bfSize,timestamp,binary
func (bf *DailyBloomFilter) dumpFile(idx int, myfile string) error {
	if bf.bfAlignSize%8 != 0 {
		return fmt.Errorf("%d must can devided by 8", bf.bfAlignSize)
	}
	f, err := os.Create(myfile)
	if err != nil {
		return err
	}
	defer f.Close()

	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, int32(bf.bms[idx].k))
	binary.Write(bytesBuffer, binary.BigEndian, int32(bf.bms[idx].m))
	binary.Write(bytesBuffer, binary.BigEndian, bf.lastTime)
	_, err = f.Write(bytesBuffer.Bytes())
	if err != nil {
		return err
	}
	for i := 0; i < bf.bms[idx].m; i += bf.bfAlignSize {
		bytesBuffer = bytes.NewBuffer([]byte{})
		var a, b uint8
		for j := 0; j < bf.bfAlignSize/8; j++ {
			a = 0
			for k := 0; k < 8; k++ {
				b = 0
				if bf.bms[idx].bitmap[i+j*8+k] {
					b = 1
				}
				a = a*2 + b
			}
			binary.Write(bytesBuffer, binary.BigEndian, a)
		}
		_, err = f.Write(bytesBuffer.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}
