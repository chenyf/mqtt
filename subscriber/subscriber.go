package subscriber

import (
	"sync"
	"unsafe"

	"go.uber.org/zap"

	"github.com/chenyf/mqttapi/mqttp"
	"github.com/chenyf/mqttapi/subscriber"

	"github.com/chenyf/mqtt/configuration"
	"github.com/chenyf/mqtt/topics"
)

// Config subscriber config options
type Config struct {
	ID             string
	OfflinePublish subscriber.Publisher
	Topics         topics.ISubscriber
	Version        mqttp.ProtocolVersion
}

// Subscriber subscriber object
type Subscriber struct {
	subscriptions subscriber.Subscriptions
	lock          sync.RWMutex
	publisher     subscriber.Publisher
	log           *zap.SugaredLogger
	access        sync.WaitGroup
	subSignal     chan topics.SubscribeResp
	unSubSignal   chan topics.UnSubscribeResp
	Config
}

var _ subscriber.IFace = (*Subscriber)(nil)

// New allocate new subscriber
func NewSubscriber(c Config) *Subscriber {
	p := &Subscriber{
		subscriptions: make(subscriber.Subscriptions),
		Config:        c,
		log:           configuration.GetLogger().Named("subscriber"),
		subSignal:     make(chan topics.SubscribeResp),
		unSubSignal:   make(chan topics.UnSubscribeResp),
	}

	p.publisher = c.OfflinePublish
	return p
}

// GetID get subscriber id
func (this *Subscriber) GetID() string {
	return this.ID
}

// Hash returns address of the provider struct.
// Used by topics provider as a key to subscriber object
func (this *Subscriber) Hash() uintptr {
	return uintptr(unsafe.Pointer(this))
}

// HasSubscriptions either has active subscriptions or not
func (this *Subscriber) HasSubscriptions() bool {
	return len(this.subscriptions) > 0
}

// Acquire prevent subscriber being deleted before active writes finished
func (this *Subscriber) Acquire() {
	this.access.Add(1)
}

// Release subscriber once topics provider finished write
func (this *Subscriber) Release() {
	this.access.Done()
}

// GetVersion return MQTT protocol version
func (this *Subscriber) GetVersion() mqttp.ProtocolVersion {
	return this.Version
}

// Subscriptions list active subscriptions
func (this *Subscriber) Subscriptions() subscriber.Subscriptions {
	return this.subscriptions
}

// Subscribe to given topic
func (this *Subscriber) Subscribe(topic string, params *subscriber.SubscriptionParams) ([]*mqttp.Publish, error) {
	resp := this.Topics.Subscribe(topics.SubscribeReq{
		Filter: topic,
		Params: params,
		S:      this,
		Chan:   this.subSignal,
	})

	if resp.Err != nil {
		return []*mqttp.Publish{}, resp.Err
	}

	params.Granted = resp.Granted
	this.subscriptions[topic] = params
	return resp.Retained, nil
}

// UnSubscribe from given topic
func (this *Subscriber) UnSubscribe(topic string) error {
	resp := this.Topics.UnSubscribe(topics.UnSubscribeReq{
		Filter: topic,
		S:      this,
		Chan:   this.unSubSignal,
	})

	delete(this.subscriptions, topic)
	return resp.Err
}

// Publish message accordingly to subscriber state
// online: forward message to session
// offline: persist message
func (this *Subscriber) Publish(p *mqttp.Publish, grantedQoS mqttp.QosType, ops mqttp.SubscriptionOptions, ids []uint32) error {
	pkt, err := p.Clone(this.Version)
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

	this.lock.RLock()
	this.publisher(this.ID, pkt)
	this.lock.RUnlock()

	return nil
}

// Online moves subscriber to online state
// since this moment all of publishes are forwarded to provided callback
func (this *Subscriber) Online(c subscriber.Publisher) {
	this.lock.Lock()
	this.publisher = c
	this.lock.Unlock()
}

// Offline put session offline
// if shutdown is true it does unsubscribe from all active subscriptions
func (this *Subscriber) Offline(shutdown bool) {
	// if session is clean then remove all remaining subscriptions
	if shutdown {
		for topic := range this.subscriptions {
			if err := this.UnSubscribe(topic); err != nil {
				this.log.Debugf("[clientId: %s] topic: %s: %s", this.ID, topic, err.Error())
			}
		}

		this.access.Wait()
		select {
		case <-this.subSignal:
		default:
			close(this.subSignal)
		}

		select {
		case <-this.unSubSignal:
		default:
			close(this.unSubSignal)
		}
	} else {
		this.lock.Lock()
		this.publisher = this.OfflinePublish
		this.lock.Unlock()
	}
}
