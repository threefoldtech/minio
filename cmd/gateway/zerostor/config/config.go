package config

import (
	"io/ioutil"

	"github.com/threefoldtech/0-stor/client"

	yaml "gopkg.in/yaml.v2"
)

const (
	//MetaTypeFile file meta data store
	MetaTypeFile = "file"
)

// Config defines non 0-stor configuration
// of minio 0-stor gateway.
type Config struct {
	client.Config `yaml:",inline"`

	// number of pipline jobs
	Jobs int `yaml:"jobs"`

	Minio struct {
		Healer struct {
			Listen string `yaml:"listen"`
		} `json:"healer"`
		TLog   *TLog `yaml:"tlog,omitempty"`
		Master *TLog `yaml:"master,omitempty"`
	} `yaml:"minio"`
}

//TLog defines a tlog config
type TLog struct {
	Address   string `yaml:"address"`
	Namespace string `yaml:"namespace"`
	Password  string `yaml:"password"`
}

//Load loads config from confFile
func Load(confFile string) (Config, error) {
	var cfg Config

	b, err := ioutil.ReadFile(confFile)
	if err != nil {
		return cfg, err
	}

	// for now we only support YAML

	err = yaml.Unmarshal(b, &cfg)
	return cfg, err
}
