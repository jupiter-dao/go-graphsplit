package config

import (
	"os"

	"github.com/BurntSushi/toml"
)

type Config struct {
	SliceSize int
}

func NewConfig() *Config {
	return &Config{
		SliceSize: 19327352832, // 18G
	}
}

func LoadConfig(filePath string) (*Config, error) {
	var cfg Config

	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_, err = toml.NewDecoder(f).Decode(&cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) SaveConfig(filePath string) error {
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := toml.NewEncoder(f)
	err = encoder.Encode(c)
	if err != nil {
		return err
	}
	return nil
}
