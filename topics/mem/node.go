package mem

import (
	"strings"

	"github.com/chenyf/mqttapi/mqttp"
	"github.com/chenyf/mqttapi/subscriber"

	"github.com/chenyf/mqtt/systree"
	"github.com/chenyf/mqtt/topics"
	"github.com/chenyf/mqtt/types"
)

type topicSubscriber struct {
	s topics.Subscriber
	p *subscriber.SubscriptionParams
}

type subscribers map[uintptr]*topicSubscriber

func (s *topicSubscriber) acquire() *publish {
	s.s.Acquire()
	pe := &publish{
		s:   s.s,
		qos: s.p.Granted,
		ops: s.p.Ops,
	}

	if s.p.ID > 0 {
		pe.ids = []uint32{s.p.ID}
	}

	return pe
}

type publish struct {
	s   topics.Subscriber
	ops mqttp.SubscriptionOptions
	qos mqttp.QosType
	ids []uint32
}

type publishes map[uintptr][]*publish

type node struct {
	retained       interface{}
	subs           subscribers
	parent         *node
	children       map[string]*node
	getSubscribers func(uintptr, *publishes)
}

func newNode(overlap bool, parent *node) *node {
	n := &node{
		subs:     make(subscribers),
		children: make(map[string]*node),
		parent:   parent,
	}

	if overlap {
		n.getSubscribers = n.overlappingSubscribers
	} else {
		n.getSubscribers = n.nonOverlappingSubscribers
	}

	return n
}

func (this *provider) leafInsertNode(levels []string) *node {
	root := this.root

	for _, level := range levels {
		// Add node if it doesn't already exist
		n, ok := root.children[level]
		if !ok {
			n = newNode(this.allowOverlapping, root)

			root.children[level] = n
		} else {

		}

		root = n
	}

	return root
}

func (this *provider) leafSearchNode(levels []string) *node {
	root := this.root

	// run down and try get path matching given topic
	for _, token := range levels {
		n, ok := root.children[token]
		if !ok {
			return nil
		}

		root = n
	}

	return root
}

func (this *provider) subscriptionInsert(filter string, sub topics.Subscriber, p *subscriber.SubscriptionParams) bool {
	levels := strings.Split(filter, "/")

	root := this.leafInsertNode(levels)

	// Let's see if the subscriber is already on the list and just update QoS if so
	// Otherwise create new entry
	exists := false
	if s, ok := root.subs[sub.Hash()]; !ok {
		root.subs[sub.Hash()] = &topicSubscriber{
			s: sub,
			p: p,
		}
	} else {
		s.p = p
		exists = true
	}

	return exists
}

func (this *provider) subscriptionRemove(topic string, sub topics.Subscriber) error {
	levels := strings.Split(topic, "/")

	var err error

	root := this.leafSearchNode(levels)
	if root == nil {
		return topics.ErrNotFound
	}

	// path matching the topic exists.
	// if subscriber argument is nil remove all of subscribers
	// otherwise try remove subscriber or set error if not exists
	if sub == nil {
		// If subscriber == nil, then it's signal to remove ALL subscribers
		root.subs = make(subscribers)
	} else {
		id := sub.Hash()
		if _, ok := root.subs[id]; ok {
			delete(root.subs, id)
		} else {
			err = topics.ErrNotFound
		}
	}

	// Run up and on each level and check if level has subscriptions and nested nodes
	// If both are empty tell parent node to remove that token
	level := len(levels)
	for leafNode := root; leafNode != nil; leafNode = leafNode.parent {
		// If there are no more subscribers or inner nodes or retained messages remove this node from parent
		if len(leafNode.subs) == 0 && len(leafNode.children) == 0 && leafNode.retained == nil {
			// if this is not root node
			this.onCleanUnsubscribe(levels[:level])
			if leafNode.parent != nil {
				delete(leafNode.parent.children, levels[level-1])
			}
		}

		level--
	}

	return err
}

func subscriptionRecurseSearch(root *node, levels []string, publishID uintptr, p *publishes) {
	if len(levels) == 0 {
		// leaf level of the topic
		// get all subscribers and return
		root.getSubscribers(publishID, p)
		if n, ok := root.children[topics.MWC]; ok {
			n.getSubscribers(publishID, p)
		}
	} else {
		if n, ok := root.children[topics.MWC]; ok && len(levels[0]) != 0 {
			n.getSubscribers(publishID, p)
		}

		if n, ok := root.children[levels[0]]; ok {
			subscriptionRecurseSearch(n, levels[1:], publishID, p)
		}

		if n, ok := root.children[topics.SWC]; ok {
			subscriptionRecurseSearch(n, levels[1:], publishID, p)
		}
	}
}

func (this *provider) subscriptionSearch(topic string, publishID uintptr, p *publishes) {
	root := this.root
	levels := strings.Split(topic, "/")
	level := levels[0]

	if !strings.HasPrefix(level, "$") {
		subscriptionRecurseSearch(root, levels, publishID, p)
	} else if n, ok := root.children[level]; ok {
		subscriptionRecurseSearch(n, levels[1:], publishID, p)
	}
}

func (this *provider) retainInsert(topic string, obj types.RetainObject) {
	levels := strings.Split(topic, "/")

	root := this.leafInsertNode(levels)

	root.retained = obj
}

func (this *provider) retainRemove(topic string) error {
	levels := strings.Split(topic, "/")

	root := this.leafSearchNode(levels)
	if root == nil {
		return topics.ErrNotFound
	}

	root.retained = nil

	// Run up and on each level and check if level has subscriptions and nested nodes
	// If both are empty tell parent node to remove that token
	level := len(levels)
	for leafNode := root; leafNode != nil; leafNode = leafNode.parent {
		// If there are no more subscribers or inner nodes or retained messages remove this node from parent
		if len(leafNode.subs) == 0 && len(leafNode.children) == 0 && leafNode.retained == nil {
			// if this is not root node
			if leafNode.parent != nil {
				delete(leafNode.parent.children, levels[level-1])
			}
		}

		level--
	}

	return nil
}

func retainRecurseSearch(root *node, levels []string, retained *[]*mqttp.Publish) {
	if len(levels) == 0 {
		// leaf level of the topic
		root.getRetained(retained)
		if n, ok := root.children[topics.MWC]; ok {
			n.allRetained(retained)
		}
	} else {
		switch levels[0] {
		case topics.MWC:
			// If '#', add all retained messages starting this node
			root.allRetained(retained)
			return
		case topics.SWC:
			// If '+', check all nodes at this level. Next levels must be matched.
			for _, n := range root.children {
				retainRecurseSearch(n, levels[1:], retained)
			}
		default:
			if n, ok := root.children[levels[0]]; ok {
				retainRecurseSearch(n, levels[1:], retained)
			}
		}
	}
}

func (this *provider) retainSearch(filter string, retained *[]*mqttp.Publish) {
	levels := strings.Split(filter, "/")
	level := levels[0]

	if level == topics.MWC {
		for t, n := range this.root.children {
			if t != "" && !strings.HasPrefix(t, "$") {
				n.allRetained(retained)
			}
		}
	} else if strings.HasPrefix(level, "$") && this.root.children[level] != nil {
		retainRecurseSearch(this.root.children[level], levels[1:], retained)
	} else {
		retainRecurseSearch(this.root, levels, retained)
	}
}

func (this *node) getRetained(retained *[]*mqttp.Publish) {
	if this.retained != nil {
		var p *mqttp.Publish

		switch t := this.retained.(type) {
		case *mqttp.Publish:
			p = t
		case systree.DynamicValue:
			p = t.Retained()
		default:
			panic("unknown retained type")
		}

		// if publish has expiration set check if there time left to live
		if _, _, expired := p.Expired(); !expired {
			*retained = append(*retained, p)
		} else {
			// publish has expired, thus nobody should get it
			this.retained = nil
		}
	}
}

func (this *node) allRetained(retained *[]*mqttp.Publish) {
	this.getRetained(retained)

	for _, n := range this.children {
		n.allRetained(retained)
	}
}

func (this *node) overlappingSubscribers(publishID uintptr, p *publishes) {
	for id, sub := range this.subs {
		if s, ok := (*p)[id]; ok {
			if sub.p.ID > 0 {
				s[0].ids = append(s[0].ids, sub.p.ID)
			}

			if s[0].qos < sub.p.Granted {
				s[0].qos = sub.p.Granted
			}
		} else {
			if !sub.p.Ops.NL() || id != publishID {
				pe := sub.acquire()
				(*p)[id] = append((*p)[id], pe)
			}
		}
	}
}

func (this *node) nonOverlappingSubscribers(publishID uintptr, p *publishes) {
	for id, sub := range this.subs {
		if !sub.p.Ops.NL() || id != publishID {
			pe := sub.acquire()
			if _, ok := (*p)[id]; ok {
				(*p)[id] = append((*p)[id], pe)
			} else {
				(*p)[id] = []*publish{pe}
			}
		}
	}
}
