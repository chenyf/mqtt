package configuration

import (
	"flag"
	"io/ioutil"
	"os"

	"github.com/chenyf/mqttapi/plugin"
	"gopkg.in/yaml.v3"
)

// PluginState status
type PluginState struct {
	Plugin plugin.Plugin
	Errors []error
}

// DefaultConfig Load minimum working configuration to allow
// server start without user provided one
func DefaultConfig() *Config {
	c := Config{}
	if err := yaml.Unmarshal(defaultConfig, &c); err != nil {
		panic(err.Error())
	}

	return &c
}

// ReadConfig read service configuration
func ReadConfig() *Config {
	log := GetHumanLogger()
	flag.Parse()

	c := DefaultConfig()

	if len(configFile) == 0 {
		log.Info("No config file provided\nuse --config option or VOLANTMQ_CONFIG environment variable to provide own")
		log.Info("default config: \n", string(defaultConfig))
	} else {
		if _, err := os.Stat(configFile); os.IsNotExist(err) {
			log.Errorf("config file \"%s\" does not exist", configFile)
			return nil
		}

		data, err := ioutil.ReadFile(configFile)
		if err != nil {
			panic(err.Error())
		}

		if err = yaml.Unmarshal(data, c); err != nil {
			panic(err.Error())
		}
	}

	return c
}
