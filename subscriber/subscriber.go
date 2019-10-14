package subscriber

import (
	"sync"
	"unsafe"

	"go.uber.org/zap"

	"github.com/chenyf/mqttapi/mqttp"
	"github.com/chenyf/mqttapi/subscriber"

	"github.com/chenyf/mqtt/configuration"
	topicsTypes "github.com/chenyf/mqtt/topics/types"
)

// Config subscriber config options
type Config struct {
	ID             string
	OfflinePublish subscriber.Publisher
	Topics         topicsTypes.ISubscriber
	Version        mqttp.ProtocolVersion
}

// Type subscriber object
type Type struct {
	subscriptions subscriber.Subscriptions
	lock          sync.RWMutex
	publisher     subscriber.Publisher
	log           *zap.SugaredLogger
	access        sync.WaitGroup
	subSignal     chan topicsTypes.SubscribeResp
	unSubSignal   chan topicsTypes.UnSubscribeResp
	Config
}

var _ subscriber.IFace = (*Type)(nil)

// New allocate new subscriber
func New(c Config) *Type {
	p := &Type{
		subscriptions: make(subscriber.Subscriptions),
		Config:        c,
		log:           configuration.GetLogger().Named("subscriber"),
		subSignal:     make(chan topicsTypes.SubscribeResp),
		unSubSignal:   make(chan topicsTypes.UnSubscribeResp),
	}

	p.publisher = c.OfflinePublish

	return p
}

// GetID get subscriber id
func (s *Type) GetID() string {
	return s.ID
}

// Hash returns address of the provider struct.
// Used by topics provider as a key to subscriber object
func (s *Type) Hash() uintptr {
	return uintptr(unsafe.Pointer(s))
}

// HasSubscriptions either has active subscriptions or not
func (s *Type) HasSubscriptions() bool {
	return len(s.subscriptions) > 0
}

// Acquire prevent subscriber being deleted before active writes finished
func (s *Type) Acquire() {
	s.access.Add(1)
}

// Release subscriber once topics provider finished write
func (s *Type) Release() {
	s.access.Done()
}

// GetVersion return MQTT protocol version
func (s *Type) GetVersion() mqttp.ProtocolVersion {
	return s.Version
}

// Subscriptions list active subscriptions
func (s *Type) Subscriptions() subscriber.Subscriptions {
	return s.subscriptions
}

// Subscribe to given topic
func (s *Type) Subscribe(topic string, params *subscriber.SubscriptionParams) ([]*mqttp.Publish, error) {
	resp := s.Topics.Subscribe(topicsTypes.SubscribeReq{
		Filter: topic,
		Params: params,
		S:      s,
		Chan:   s.subSignal,
	})

	if resp.Err != nil {
		return []*mqttp.Publish{}, resp.Err
	}

	params.Granted = resp.Granted

	s.subscriptions[topic] = params

	return resp.Retained, nil
}

// UnSubscribe from given topic
func (s *Type) UnSubscribe(topic string) error {
	resp := s.Topics.UnSubscribe(topicsTypes.UnSubscribeReq{
		Filter: topic,
		S:      s,
		Chan:   s.unSubSignal,
	})

	delete(s.subscriptions, topic)

	return resp.Err
}

// Publish message accordingly to subscriber state
// online: forward message to session
// offline: persist message
func (s *Type) Publish(p *mqttp.Publish, grantedQoS mqttp.QosType, ops mqttp.SubscriptionOptions, ids []uint32) error {
	pkt, err := p.Clone(s.Version)
	if err != nil {
		return err
	}

	if len(ids) > 0 {
		if err = pkt.PropertySet(mqttp.PropertySubscriptionIdentifier, ids); err != nil {
			return err
		}
	}

	if !ops.RAP() {
		pkt.SetRetain(false)
	}

	if pkt.QoS() != mqttp.QoS0 {
		pkt.SetPacketID(0)
	}

	switch grantedQoS {
	// If a subscribing Client has been granted maximum QoS 1 for a particular Topic Filter, then a
	// QoS 0 Application Message matching the filter is delivered to the Client at QoS 0. This means
	// that at most one copy of the message is received by the Client. On the other hand, a QoS 2
	// Message published to the same topic is downgraded by the Server to QoS 1 for delivery to the
	// Client, so that Client might receive duplicate copies of the Message.
	case mqttp.QoS1:
		if pkt.QoS() == mqttp.QoS2 {
			_ = pkt.SetQoS(mqttp.QoS1) // nolint: errcheck
		}

		// If the subscribing Client has been granted maximum QoS 0, then an Application Message
		// originally published as QoS 2 might get lost on the hop to the Client, but the Server should never
		// send a duplicate of that Message. A QoS 1 Message published to the same topic might either get
		// lost or duplicated on its transmission to that Client.
		// case message.QoS0:
	}

	s.lock.RLock()
	s.publisher(s.ID, pkt)
	s.lock.RUnlock()

	return nil
}

// Online moves subscriber to online state
// since this moment all of publishes are forwarded to provided callback
func (s *Type) Online(c subscriber.Publisher) {
	s.lock.Lock()
	s.publisher = c
	s.lock.Unlock()
}

// Offline put session offline
// if shutdown is true it does unsubscribe from all active subscriptions
func (s *Type) Offline(shutdown bool) {
	// if session is clean then remove all remaining subscriptions
	if shutdown {
		for topic := range s.subscriptions {
			if err := s.UnSubscribe(topic); err != nil {
				s.log.Debugf("[clientId: %s] topic: %s: %s", s.ID, topic, err.Error())
			}
		}

		s.access.Wait()
		select {
		case <-s.subSignal:
		default:
			close(s.subSignal)
		}

		select {
		case <-s.unSubSignal:
		default:
			close(s.unSubSignal)
		}
	} else {
		s.lock.Lock()
		s.publisher = s.OfflinePublish
		s.lock.Unlock()
	}
}
