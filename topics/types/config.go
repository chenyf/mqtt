package topicsTypes

import (
	"github.com/chenyf/mqttapi/mqttp"
	"github.com/chenyf/mqttapi/vlplugin/vlpersistence"
	"github.com/chenyf/mqtt/systree"
)

// ProviderConfig interface implemented by every backend
type ProviderConfig interface{}

// MemConfig of topics manager
type MemConfig struct {
	Stat                     systree.TopicsStat
	Persist                  vlpersistence.Retained
	OnCleanUnsubscribe       func([]string)
	Name                     string
	MaxQos                   mqttp.QosType
	OverlappingSubscriptions bool
}

// NewMemConfig generate default config for memory
func NewMemConfig() *MemConfig {
	return &MemConfig{
		Name:                     "mem",
		MaxQos:                   mqttp.QoS2,
		OnCleanUnsubscribe:       func([]string) {},
		OverlappingSubscriptions: false,
	}
}
