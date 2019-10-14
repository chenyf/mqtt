// Copyright (c) 2014 The VolantMQ Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mem

import (
	"sync"
	"time"

	"github.com/chenyf/mqttapi/mqttp"
	"github.com/chenyf/mqttapi/plugin/persist"
	"github.com/chenyf/mqttapi/subscriber"
	"go.uber.org/zap"

	"github.com/chenyf/mqtt/configuration"
	"github.com/chenyf/mqtt/systree"
	"github.com/chenyf/mqtt/topics"
	"github.com/chenyf/mqtt/types"
)

type provider struct {
	smu                sync.RWMutex
	root               *node
	stat               systree.TopicsStat
	persist            persist.Retained
	log                *zap.SugaredLogger
	onCleanUnsubscribe func([]string)
	wgPublisher        sync.WaitGroup
	wgPublisherStarted sync.WaitGroup
	inbound            chan *mqttp.Publish
	inRetained         chan types.RetainObject
	subIn              chan topics.SubscribeReq
	unSubIn            chan topics.UnSubscribeReq
	allowOverlapping   bool
}

var _ topics.Provider = (*provider)(nil)

// NewMemProvider returns an new instance of the provider, which is implements the
// TopicsProvider interface. provider is a hidden struct that stores the topic
// subscriptions and retained messages in memory. The content is not persistent so
// when the server goes, everything will be gone. Use with care.
func NewMemProvider(config *topics.MemConfig) (topics.Provider, error) {
	p := &provider{
		stat:               config.Stat,
		persist:            config.Persist,
		onCleanUnsubscribe: config.OnCleanUnsubscribe,
		inbound:            make(chan *mqttp.Publish, 1024*512),
		inRetained:         make(chan types.RetainObject, 1024*512),
		subIn:              make(chan topics.SubscribeReq, 1024*512),
		unSubIn:            make(chan topics.UnSubscribeReq, 1024*512),
	}
	p.root = newNode(p.allowOverlapping, nil)

	p.log = configuration.GetLogger().Named("topics").Named(config.Name)

	if p.persist != nil {
		entries, err := p.persist.Load()
		if err != nil && err != persist.ErrNotFound {
			return nil, err
		}

		for _, d := range entries {
			v := mqttp.ProtocolVersion(d.Data[0])
			var pkt mqttp.IFace
			pkt, _, err = mqttp.Decode(v, d.Data[1:])
			if err != nil {
				p.log.Error("Couldn't decode retained message", zap.Error(err))
			} else {
				if m, ok := pkt.(*mqttp.Publish); ok {
					if len(d.ExpireAt) > 0 {
						var tm time.Time
						if tm, err = time.Parse(time.RFC3339, d.ExpireAt); err == nil {
							m.SetExpireAt(tm)
						} else {
							p.log.Error("Decode publish expire at", zap.Error(err))
						}
					}
					_ = p.Retain(m) // nolint: errcheck
				} else {
					p.log.Warn("Unsupported retained message type", zap.String("type", m.Type().Name()))
				}
			}
		}
	}

	publisherCount := 1
	subsCount := 1
	unSunCount := 1

	p.wgPublisher.Add(publisherCount + subsCount + unSunCount + 1)
	p.wgPublisherStarted.Add(publisherCount + subsCount + unSunCount + 1)

	for i := 0; i < publisherCount; i++ {
		go p.publisher()
	}

	go p.retainer()

	for i := 0; i < subsCount; i++ {
		go p.subscriber()
	}

	for i := 0; i < unSunCount; i++ {
		go p.unSubscriber()
	}

	p.wgPublisherStarted.Wait()

	return p, nil
}

func (this *provider) Subscribe(req topics.SubscribeReq) topics.SubscribeResp {
	cAllocated := false

	if req.Chan == nil {
		cAllocated = true
		req.Chan = make(chan topics.SubscribeResp)
	}

	// 将请求送入处理队列
	this.subIn <- req

	// 同步等待处理结果
	resp := <-req.Chan

	if cAllocated {
		close(req.Chan)
	}

	return resp
}

func (this *provider) UnSubscribe(req topics.UnSubscribeReq) topics.UnSubscribeResp {
	cAllocated := false

	if req.Chan == nil {
		cAllocated = true
		req.Chan = make(chan topics.UnSubscribeResp)
	}

	this.unSubIn <- req

	resp := <-req.Chan

	if cAllocated {
		close(req.Chan)
	}

	return resp
}

func (this *provider) subscribe(filter string, s topics.Subscriber, p *subscriber.SubscriptionParams) ([]*mqttp.Publish, error) {
	defer this.smu.Unlock()
	this.smu.Lock()

	if s == nil || p == nil {
		return []*mqttp.Publish{}, topics.ErrInvalidArgs
	}

	p.Granted = p.Ops.QoS()
	exists := this.subscriptionInsert(filter, s, p)

	var r []*mqttp.Publish

	// [MQTT-3.3.1-5]
	rh := p.Ops.RetainHandling()
	if (rh == mqttp.RetainHandlingRetain) || ((rh == mqttp.RetainHandlingIfNotExists) && !exists) {
		this.retainSearch(filter, &r)
	}

	return r, nil
}

func (this *provider) unSubscribe(topic string, sub topics.Subscriber) error {
	defer this.smu.Unlock()
	this.smu.Lock()

	return this.subscriptionRemove(topic, sub)
}

func (this *provider) Publish(m interface{}) error {
	msg, ok := m.(*mqttp.Publish)
	if !ok {
		return topics.ErrUnexpectedObjectType
	}
	this.inbound <- msg

	return nil
}

func (this *provider) Retain(obj types.RetainObject) error {
	this.inRetained <- obj

	return nil
}

func (this *provider) Retained(filter string) ([]*mqttp.Publish, error) {
	// [MQTT-3.3.1-5]
	var r []*mqttp.Publish

	defer this.smu.Unlock()
	this.smu.Lock()

	// [MQTT-3.3.1-5]
	this.retainSearch(filter, &r)

	return r, nil
}

func (this *provider) Shutdown() error {
	defer this.smu.Unlock()
	this.smu.Lock()

	close(this.inbound)
	close(this.inRetained)
	close(this.subIn)
	close(this.unSubIn)

	this.wgPublisher.Wait()

	if this.persist != nil {
		var res []*mqttp.Publish
		// [MQTT-3.3.1-5]
		this.retainSearch("#", &res)
		this.retainSearch("/#", &res)

		var encoded []*persist.PersistedPacket

		for _, pkt := range res {
			// Discard retained expired and QoS0 messages
			if expireAt, _, expired := pkt.Expired(); !expired && pkt.QoS() != mqttp.QoS0 {
				if buf, err := mqttp.Encode(pkt); err != nil {
					this.log.Error("Couldn't encode retained message", zap.Error(err))
				} else {
					entry := &persist.PersistedPacket{
						Data: buf,
					}
					if !expireAt.IsZero() {
						entry.ExpireAt = expireAt.Format(time.RFC3339)
					}
					encoded = append(encoded, entry)
				}
			}
		}
		if len(encoded) > 0 {
			this.log.Debug("Storing retained messages", zap.Int("amount", len(encoded)))
			if err := this.persist.Store(encoded); err != nil {
				this.log.Error("Couldn't persist retained messages", zap.Error(err))
			}
		}
	}

	this.root = nil
	return nil
}

func (this *provider) retain(obj types.RetainObject) {
	insert := true

	this.smu.Lock()

	switch t := obj.(type) {
	case *mqttp.Publish:
		// [MQTT-3.3.1-10]            [MQTT-3.3.1-7]
		if len(t.Payload()) == 0 || t.QoS() == mqttp.QoS0 {
			_ = this.retainRemove(obj.Topic()) // nolint: errcheck
			if len(t.Payload()) == 0 {
				insert = false
			}
		}
	}

	if insert {
		this.retainInsert(obj.Topic(), obj)
	}

	this.smu.Unlock()
}

func (this *provider) subscriber() {
	defer this.wgPublisher.Done()
	this.wgPublisherStarted.Done()

	for req := range this.subIn {
		retained, _ := this.subscribe(req.Filter, req.S, req.Params)
		req.Chan <- topics.SubscribeResp{Retained: retained}
	}
}

func (this *provider) unSubscriber() {
	defer this.wgPublisher.Done()
	this.wgPublisherStarted.Done()

	for req := range this.unSubIn {
		req.Chan <- topics.UnSubscribeResp{Err: this.unSubscribe(req.Filter, req.S)}
	}
}

func (this *provider) retainer() {
	defer this.wgPublisher.Done()
	this.wgPublisherStarted.Done()

	for obj := range this.inRetained {
		this.retain(obj)
	}
}

func (this *provider) publisher() {
	defer this.wgPublisher.Done()
	this.wgPublisherStarted.Done()

	for msg := range this.inbound {
		pubEntries := publishes{}

		this.smu.Lock()
		this.subscriptionSearch(msg.Topic(), msg.PublishID(), &pubEntries)
		this.smu.Unlock()

		for _, pub := range pubEntries {
			for _, e := range pub {
				if err := e.s.Publish(msg, e.qos, e.ops, e.ids); err != nil {
					this.log.Error("Publish error", zap.Error(err))
				}
				e.s.Release()
			}
		}
	}
}
