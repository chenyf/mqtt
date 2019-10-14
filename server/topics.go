package server

import (
	"github.com/chenyf/mqtt/topics"
	"github.com/chenyf/mqtt/topics/memLockFree"
)

// New topic provider
func NewTopicManager(config topics.ProviderConfig) (topics.Provider, error) {
	if config == nil {
		return nil, topics.ErrInvalidArgs
	}

	switch cfg := config.(type) {
	case *topics.MemConfig:
		return memLockFree.NewMemProvider(cfg)
	default:
		return nil, topics.ErrUnknownProvider
	}
}


