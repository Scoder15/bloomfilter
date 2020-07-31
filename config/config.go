package config

import (
	"io/ioutil"
	"time"

	yaml "gopkg.in/yaml.v2"
)

// Config represent the yaml structure of tokenizer config file.
type Config struct {
	BloomFilter    BloomFilterConfig `yaml:"bloomfilter"`
	InactiveFilter BloomFilterConfig `yaml:"inactiveFilter"`
	SyncCount      int               `yaml:"syncCount"`
	SyncMinCount   int               `yaml:"syncMinCount"`
}

// BloomFilterConfig represents bloom filter configs.
type BloomFilterConfig struct {
	HashNum       int           `yaml:"hashNum"`
	BfNum         int           `yaml:"bfNum"`
	BfSize        int           `yaml:"bfSize"`
	BfPeriod      int64         `yaml:"bfPeriod"`
	BfDumpDir     string        `yaml:"bfDumpDir"`
	FlushInterval time.Duration `yaml:"flushInterval"`
}

// Load loads the config file and returns the configs.
func Load(filepath string) (*Config, error) {
	configs, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	var conf Config
	err = yaml.Unmarshal(configs, &conf)
	if err != nil {
		return nil, err
	}
	return &conf, nil
}
