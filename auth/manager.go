package auth

import (
	"errors"
	"fmt"

	"github.com/chenyf/mqttapi/plugin/auth"
)

// Manager auth
type Manager struct {
	p         []auth.IFace
	anonymous bool
}

var providers = make(map[string]auth.IFace)

// Register auth provider
func Register(name string, i auth.IFace) error {
	if name == "" && i == nil {
		return errors.New("invalid args")
	}

	if _, dup := providers[name]; dup {
		return errors.New("already exists")
	}

	providers[name] = i

	return nil
}

// UnRegister authenticator
func UnRegister(name string) {
	delete(providers, name)
}

// NewManager new auth manager
func NewManager(p []string, allowAnonymous bool) (*Manager, error) {
	m := Manager{
		anonymous: allowAnonymous,
	}

	for _, pa := range p {
		pvd, ok := providers[pa]
		if !ok {
			return nil, fmt.Errorf("session: unknown provider %q", pa)
		}

		m.p = append(m.p, pvd)
	}

	return &m, nil
}

// AllowAnonymous allow anonymous connections
func (m *Manager) AllowAnonymous() error {
	if m.anonymous {
		return auth.StatusAllow
	}

	return auth.StatusDeny
}

// Password authentication
func (m *Manager) Password(clientID, user, password string) error {
	if user == "" && m.anonymous {
		return auth.StatusAllow
	} else {
		for _, p := range m.p {
			if status := p.Password(clientID, user, password); status == auth.StatusAllow {
				return status
			}
		}
	}

	return auth.StatusDeny
}

// ACL check permissions
func (m *Manager) ACL(clientID, user, topic string, access auth.AccessType) error {
	for _, p := range m.p {
		if status := p.ACL(clientID, user, topic, access); status == auth.StatusAllow {
			return status
		}
	}

	return auth.StatusDeny
}
