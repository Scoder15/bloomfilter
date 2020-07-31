package models

import (
	"bloomfilter/common"
	"errors"
	"os"

	"github.com/donnie4w/go-logger/logger"
)

const (
	dumpDataFilePrefix = "bfdump"
)

// BloomFilterManager represents a bloomfilter manager, it
// manages the loading and flush of bloomfilter data.
type BloomFilterManager struct {
	bloomfilter *common.DailyBloomFilter
	dataDir     string
}

// NewBloomFilterManager returns a bloomfilter manager.
func NewBloomFilterManager(dumpDir string, bfNum, hashNum, bfSize int,
	bfPeriod int64) *BloomFilterManager {
	bf := common.NewDailyBloomFilter(bfNum, hashNum, bfSize, bfPeriod)
	return &BloomFilterManager{
		dataDir:     dumpDir,
		bloomfilter: bf,
	}
}

// LoadBloomFilter loads the bloomfilter data stored on disk.
func (mgr *BloomFilterManager) loadBloomFilter() error {
	filepath := mgr.dataDir + "/" + dumpDataFilePrefix
	return mgr.bloomfilter.Load(filepath)
}

// FlushBloomFilter flushes bloomfilter data to disk.
func (mgr *BloomFilterManager) flushBloomFilter() error {
	filepath := mgr.dataDir + "/" + dumpDataFilePrefix
	return mgr.bloomfilter.Dump(filepath)
}

// Add adds given item to bloomfilter.
func (mgr *BloomFilterManager) Add(item []byte) {
	if len(item) > 0 {
		mgr.bloomfilter.Add(item)
	}
}

// Check returns true if given item already in the bloomfilter.
func (mgr *BloomFilterManager) Check(item []byte) bool {
	return mgr.bloomfilter.Check(item)
}

// CheckRecentN returns true if given item already in the recent n bloomfilter.
func (mgr *BloomFilterManager) CheckRecentN(item []byte, n int) bool {
	return mgr.bloomfilter.CheckRecentN(item, n)
}

// Load loads the bloomfilter data into memory.
func (mgr *BloomFilterManager) Load() error {
	var err error
	if mgr.dataDir == "" {
		return errors.New("invalid data directory")
	}
	exist, _ := FileExist(mgr.dataDir)
	if !exist {
		err = os.MkdirAll(mgr.dataDir, 0644)
		if err != nil {
			logger.Error("create bloomfilter manager directory failure,",
				" error:", err.Error())
			return err
		}
		logger.Info("empty bloomfilter manager directory created,",
			" path:", mgr.dataDir)
		return nil
	}
	err = mgr.loadBloomFilter()
	if err != nil {
		logger.Error("loading bloomfilter data failure,",
			" error:", err.Error())
		return err
	}
	return nil
}

// Flush flushes the bloomfilter data to disk.
func (mgr *BloomFilterManager) Flush() error {
	err := mgr.flushBloomFilter()
	if err != nil {
		logger.Error("flush bloomfilter data failure,",
			" error:", err.Error())
		return err
	}
	return nil
}

// FileExist returns true if the given file or directory path exists.
func FileExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
