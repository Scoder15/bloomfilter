package main

import (
	"bloomfilter/config"
	"bloomfilter/models"
	"log"
)

var (
	bloomfilterMgr           *models.BloomFilterManager
	inactiveFilterMgr        *models.BloomFilterManager
	filterConf               *config.Config
	stopUpdater, stopFlusher chan bool
)

func init() {
	var err error
	// Load server configs.
	filterConf, err = config.Load("conf/pokefilter.yml")
	if err != nil {
		log.Println("load config failure,",
			" error:", err.Error())
	}
	bloomfilterMgr = models.NewBloomFilterManager(
		filterConf.BloomFilter.BfDumpDir,
		filterConf.BloomFilter.BfNum,
		filterConf.BloomFilter.HashNum,
		filterConf.BloomFilter.BfSize,
		filterConf.BloomFilter.BfPeriod,
	)
	err = bloomfilterMgr.Load()
	if err != nil {
		log.Println("load bloomfilter failure,",
			" error:", err.Error())
	}
	log.Println("load bloomfilter failure")

}
func main() {
	log.Println("helloworld")
}
