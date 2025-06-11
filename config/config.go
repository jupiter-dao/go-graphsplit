package config

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/BurntSushi/toml"
)

type Config struct {
	SliceSize               int    `toml:"SliceSize" comment:"SliceSize, the size of each slice in bytes, default is 18G"`
	ExtraFilePath           string `toml:"ExtraFilePath" comment:"ExtraFilePath extra file path, 指向存储了图片、视频等文件的目录"`
	ExtraFileSizeInOnePiece string `toml:"ExtraFileSizeInOnePiece" comment:"ExtraFileSizeInOnePiece 每个 piece 文件包含图片和视频等文件的大小, 例如：500Mib"`
}

func NewConfig() *Config {
	return &Config{
		SliceSize:               19327352832, // 18G
		ExtraFileSizeInOnePiece: "",
		ExtraFilePath:           "",
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

func generateTOMLWithComments(data any) (string, error) {
	// Step 1: Encode struct to TOML
	var buf bytes.Buffer
	if err := toml.NewEncoder(&buf).Encode(data); err != nil {
		return "", fmt.Errorf("failed to encode TOML: %v", err)
	}
	tomlLines := strings.Split(buf.String(), "\n")

	// Step 2: Get field comments using reflection
	comments := make(map[string]string)
	val := reflect.ValueOf(data)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	typ := val.Type()

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tomlTag := field.Tag.Get("toml")
		commentTag := field.Tag.Get("comment")
		if tomlTag != "" && commentTag != "" {
			comments[tomlTag] = commentTag
		}
	}

	// Step 3: Insert comments before corresponding TOML keys
	var result []string
	for _, line := range tomlLines {
		// Skip empty lines
		if strings.TrimSpace(line) == "" {
			result = append(result, line)
			continue
		}

		// Check if the line contains a TOML key
		for key, comment := range comments {
			if strings.HasPrefix(strings.TrimSpace(line), key+" =") {
				result = append(result, fmt.Sprintf("# %s", comment))
			}
		}
		result = append(result, line)
	}

	// Add a header comment
	header := []string{
		"# 配置文件",
		"# 自动生成，包含字段说明",
		"",
	}
	return strings.Join(append(header, result...), "\n"), nil
}
