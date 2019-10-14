package memLockFree

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/chenyf/mqttapi/mqttp"
	"github.com/chenyf/mqttapi/subscriber"

	"github.com/chenyf/mqtt/systree"
	"github.com/chenyf/mqtt/topics"
	"github.com/chenyf/mqtt/types"
)

type topicSubscriber struct {
	s topics.Subscriber
	p *subscriber.SubscriptionParams
	sync.RWMutex
}

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

type retainer struct {
	val interface{}
}

type node struct {
	retained  atomic.Value
	wgDeleted sync.WaitGroup
	subs      sync.Map
	children  sync.Map
	kidsCount int32
	subsCount int32
	remove    int32
	parent    *node
}

func newNode(parent *node) *node {
	n := &node{
		parent: parent,
	}

	n.retained.Store(retainer{})

	return n
}

func (this *provider) leafInsertNode(levels []string) *node {
	root := this.root

	for i, level := range levels {
		for {
			atomic.AddInt32(&root.kidsCount, 1)

			value, ok := root.children.LoadOrStore(level, newNode(root))
			n := value.(*node)

			atomic.AddInt32(&n.subsCount, 1)

			if ok {
				atomic.AddInt32(&root.kidsCount, -1)
				if atomic.LoadInt32(&n.remove) == 1 {
					atomic.AddInt32(&n.subsCount, -1)
					n.wgDeleted.Wait()
					continue
				}
			}

			if i != len(levels)-1 {
				atomic.AddInt32(&n.subsCount, -1)
			}
			root = n
			break
		}
	}

	return root
}

func (this *provider) leafSearchNode(levels []string) *node {
	root := this.root

	// run down and try get path matching given topic
	for _, token := range levels {
		n, ok := root.children.Load(token)
		if !ok {
			return nil
		}

		root = n.(*node)
	}

	return root
}

func (this *provider) subscriptionInsert(filter string, sub topics.Subscriber, p *subscriber.SubscriptionParams) bool {
	levels := strings.Split(filter, "/")

	leaf := this.leafInsertNode(levels)

	// Let's see if the subscriber is already on the list and just update QoS if so
	// Otherwise create new entry
	if s, ok := leaf.subs.LoadOrStore(sub.Hash(), &topicSubscriber{s: sub, p: p}); ok {
		atomic.AddInt32(&leaf.subsCount, 1)
		ts := s.(*topicSubscriber)
		if ok {
			ts.Lock()
			ts.p = p
			ts.Unlock()
		}

		return true
	}

	return false
}

func (this *provider) subscriptionRemove(topic string, sub topics.Subscriber) error {
	levels := strings.Split(topic, "/")

	var err error

	leaf := this.leafSearchNode(levels)
	if leaf == nil {
		return topics.ErrNotFound
	}

	// path matching the topic exists.
	// if subscriber argument is nil remove all of subscribers
	// otherwise try remove subscriber or set error if not exists
	if sub == nil {
		// If subscriber == nil, then it's signal to remove ALL subscribers
		leaf.subs.Range(func(key, value interface{}) bool {
			atomic.AddInt32(&leaf.subsCount, -1)
			leaf.subs.Delete(key)
			return true
		})
	} else {
		if _, ok := leaf.subs.Load(sub.Hash()); !ok {
			err = topics.ErrNotFound
		} else {
			atomic.AddInt32(&leaf.subsCount, -1)
			leaf.subs.Delete(sub.Hash())
		}
	}

	this.nodesCleanup(leaf, levels)

	return err
}

func (this *provider) nodesCleanup(root *node, levels []string) {
	// Run up and on each level and check if level has subscriptions and nested nodes
	// If both are empty tell parent node to remove that token
	level := len(levels)

	for leafNode := root; leafNode != nil; leafNode = leafNode.parent {
		// If there are no more subscribers or inner nodes or retained messages remove this node from parent
		if atomic.LoadInt32(&leafNode.subsCount) == 0 &&
			atomic.LoadInt32(&leafNode.kidsCount) == 0 &&
			leafNode.retained.Load().(retainer).val == nil {
			leafNode.wgDeleted.Add(1)
			atomic.StoreInt32(&leafNode.remove, 1)

			if atomic.LoadInt32(&leafNode.subsCount) == 0 &&
				atomic.LoadInt32(&leafNode.kidsCount) == 0 &&
				leafNode.retained.Load().(retainer).val == nil {
				this.onCleanUnsubscribe(levels[:level])
				// if this is not root node
				if leafNode.parent != nil {
					leafNode.parent.children.Delete(levels[level-1])
				}
			}

			leafNode.wgDeleted.Done()
		}

		level--
	}
}

func (this *provider) subscriptionRecurseSearch(root *node, levels []string, publishID uintptr, p *publishes) {
	if len(levels) == 0 {
		// leaf level of the topic
		// get all subscribers and return
		this.nodeSubscribers(root, publishID, p)
		if n, ok := root.children.Load(topics.MWC); ok {
			this.nodeSubscribers(n.(*node), publishID, p)
		}
	} else {
		if n, ok := root.children.Load(topics.MWC); ok && len(levels[0]) != 0 {
			this.nodeSubscribers(n.(*node), publishID, p)
		}

		if n, ok := root.children.Load(levels[0]); ok {
			this.subscriptionRecurseSearch(n.(*node), levels[1:], publishID, p)
		}

		if n, ok := root.children.Load(topics.SWC); ok {
			this.subscriptionRecurseSearch(n.(*node), levels[1:], publishID, p)
		}
	}
}

func (this *provider) subscriptionSearch(topic string, publishID uintptr, p *publishes) {
	root := this.root
	levels := strings.Split(topic, "/")
	level := levels[0]

	if !strings.HasPrefix(level, "$") {
		this.subscriptionRecurseSearch(root, levels, publishID, p)
	} else if n, ok := root.children.Load(level); ok {
		this.subscriptionRecurseSearch(n.(*node), levels[1:], publishID, p)
	}
}

func (this *provider) retainInsert(topic string, obj types.RetainObject) {
	levels := strings.Split(topic, "/")

	root := this.leafInsertNode(levels)

	root.retained.Store(retainer{val: obj})
	atomic.AddInt32(&root.subsCount, -1)
}

func (this *provider) retainRemove(topic string) error {
	levels := strings.Split(topic, "/")

	root := this.leafSearchNode(levels)
	if root == nil {
		return topics.ErrNotFound
	}

	root.retained.Store(retainer{})

	this.nodesCleanup(root, levels)

	return nil
}

func retainRecurseSearch(root *node, levels []string, retained *[]*mqttp.Publish) {
	if len(levels) == 0 {
		// leaf level of the topic
		root.getRetained(retained)
		if value, ok := root.children.Load(topics.MWC); ok {
			n := value.(*node)
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
			root.children.Range(func(key, value interface{}) bool {
				retainRecurseSearch(value.(*node), levels[1:], retained)

				return true
			})
		default:
			if value, ok := root.children.Load(levels[0]); ok {
				retainRecurseSearch(value.(*node), levels[1:], retained)
			}
		}
	}
}

func (this *provider) retainSearch(filter string, retained *[]*mqttp.Publish) {
	levels := strings.Split(filter, "/")
	level := levels[0]

	if level == topics.MWC {
		this.root.children.Range(func(key, value interface{}) bool {
			t := key.(string)
			n := value.(*node)

			if t != "" && !strings.HasPrefix(t, "$") {
				n.allRetained(retained)
			}

			return true
		})
	} else if strings.HasPrefix(level, "$") {
		value, ok := this.root.children.Load(level)
		var n *node
		if ok {
			n = value.(*node)
		}
		retainRecurseSearch(n, levels[1:], retained)
	} else {
		retainRecurseSearch(this.root, levels, retained)
	}
}

func (this *node) getRetained(retained *[]*mqttp.Publish) {
	rt := this.retained.Load().(retainer)

	switch val := rt.val.(type) {
	case types.RetainObject:
		var p *mqttp.Publish

		switch t := val.(type) {
		case *mqttp.Publish:
			p = t
		case systree.DynamicValue:
			p = t.Retained()
		default:
			panic("unknown retain type")
		}

		// if publish has expiration set check if there time left to live
		if _, _, expired := p.Expired(); !expired {
			*retained = append(*retained, p)
		} else {
			// publish has expired, thus nobody should get it
			this.retained.Store(retainer{})
		}
	}
}

func (this *node) allRetained(retained *[]*mqttp.Publish) {
	this.getRetained(retained)

	this.children.Range(func(key, value interface{}) bool {
		n := value.(*node)
		n.allRetained(retained)
		return true
	})
}

func overlappingSubscribers(this *node, publishID uintptr, p *publishes) {
	this.subs.Range(func(key, value interface{}) bool {
		id := key.(uintptr)
		sub := value.(*topicSubscriber)

		if s, ok := (*p)[id]; ok {
			if sub.p.ID > 0 {
				s[0].ids = append(s[0].ids, sub.p.ID)
			}

			if s[0].qos < sub.p.Granted {
				s[0].qos = sub.p.Granted
			}
		} else {
			sub.RLock()
			if !sub.p.Ops.NL() || id != publishID {
				pe := sub.acquire()
				(*p)[id] = append((*p)[id], pe)
			}
			sub.RUnlock()
		}

		return true
	})
}

func nonOverlappingSubscribers(this *node, publishID uintptr, p *publishes) {
	this.subs.Range(func(key, value interface{}) bool {
		id := key.(uintptr)
		sub := value.(*topicSubscriber)

		if !sub.p.Ops.NL() || id != publishID {
			pe := sub.acquire()
			if _, ok := (*p)[id]; ok {
				(*p)[id] = append((*p)[id], pe)
			} else {
				(*p)[id] = []*publish{pe}
			}
		}

		return true
	})
}
