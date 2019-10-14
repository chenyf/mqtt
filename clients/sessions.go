package clients

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/chenyf/mqttapi/plugin/persist"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"

	"github.com/chenyf/mqttapi/mqttp"
	apiauth "github.com/chenyf/mqttapi/plugin/auth"
	apisubscriber "github.com/chenyf/mqttapi/subscriber"
	"go.uber.org/zap"

	"github.com/chenyf/mqtt/auth"
	"github.com/chenyf/mqtt/configuration"
	"github.com/chenyf/mqtt/connection"
	"github.com/chenyf/mqtt/subscriber"
	"github.com/chenyf/mqtt/systree"
	"github.com/chenyf/mqtt/topics"
	"github.com/chenyf/mqtt/transport"
	"github.com/chenyf/mqtt/types"
)

// load sessions owning subscriptions
type subscriberConfig struct {
	version mqttp.ProtocolVersion
	topics  apisubscriber.Subscriptions
}

// Config manager configuration
type Config struct {
	configuration.MqttConfig
	TopicsMgr        topics.Provider
	Persist          persist.IFace
	Systree          systree.Provider
	OnReplaceAttempt func(string, bool)
	NodeName         string
}

type preloadConfig struct {
	exp *expiryConfig
	sub *subscriberConfig
}

// Manager clients manager
type Manager struct {
	persistence     persist.Sessions
	log             *zap.SugaredLogger
	quit            chan struct{}
	sessionsCount   sync.WaitGroup
	expiryCount     sync.WaitGroup
	sessions        sync.Map
	plSubscribers   map[string]apisubscriber.IFace
	allowedVersions map[mqttp.ProtocolVersion]bool
	Config
}

// StartConfig used to reconfigure session after connection is created
type StartConfig struct {
	Req  *mqttp.Connect
	Resp *mqttp.ConnAck
	Conn net.Conn
	Auth apiauth.Permissions
}

type containerInfo struct {
	ses     *session
	sub     *subscriber.Subscriber
	present bool
}

type loadContext struct {
	bar            *mpb.Bar
	startTs        time.Time
	preloadConfigs map[string]*preloadConfig
	delayedWills   []mqttp.IFace
}

// NewManager create new clients manager
func NewManager(c *Config) (*Manager, error) {
	var err error

	var mgr *Manager

	defer func() {
		if err != nil {

		}
	}()

	mgr = &Manager{
		Config: *c,
		quit:   make(chan struct{}),
		log:    configuration.GetLogger().Named("sessions"),
		allowedVersions: map[mqttp.ProtocolVersion]bool{
			mqttp.ProtocolV31:  false,
			mqttp.ProtocolV311: false,
			mqttp.ProtocolV50:  false,
		},
		plSubscribers: make(map[string]apisubscriber.IFace),
	}

	mgr.persistence, _ = c.Persist.Sessions()

	for _, v := range mgr.Version {
		switch v {
		case "v3.1":
			mgr.allowedVersions[mqttp.ProtocolV31] = true
		case "v3.1.1":
			mgr.allowedVersions[mqttp.ProtocolV311] = true
		case "v5.0":
			mgr.allowedVersions[mqttp.ProtocolV50] = true
		default:
			return nil, errors.New("unknown MQTT protocol: " + v)
		}
	}

	pCount := mgr.persistence.Count()

	if pCount > 0 {
		pBars := mpb.New(mpb.WithWidth(64))
		bar := pBars.AddBar(int64(pCount),
			mpb.BarClearOnComplete(),
			mpb.PrependDecorators(
				decor.Name("persisted", decor.WC{W: len("persisted") + 1, C: decor.DSyncSpaceR}),
				decor.CountersNoUnit("%d / %d", decor.WCSyncWidth),
				decor.OnComplete(decor.Name("", decor.WCSyncSpaceR), " done!"),
			),
			mpb.AppendDecorators(
				decor.Percentage(decor.WC{W: 5})),
		)

		mgr.log.Info("Loading sessions. Might take a while")
		_ = mgr.log.Sync()

		context := &loadContext{
			bar:            bar,
			preloadConfigs: make(map[string]*preloadConfig),
		}

		// load sessions for fill systree
		// those sessions having either will delay or expire are created with and timer started
		err = mgr.persistence.LoadForEach(mgr, context)

		if !bar.Completed() {
			bar.Abort(false)
		}

		pBars.Wait()

		fmt.Printf("\n")

		if err != nil {
			return nil, err
		}

		mgr.configurePersistedSubscribers(context)
		mgr.configurePersistedExpiry(context)
		mgr.processDelayedWills(context)

		for id, st := range context.preloadConfigs {
			if st.sub != nil {
				_ = mgr.persistence.SubscriptionsDelete([]byte(id))
			}
			if st.exp != nil {
				_ = mgr.persistence.ExpiryDelete([]byte(id))
			}
		}

		mgr.log.Info("Sessions loaded")
	} else {
		mgr.log.Info("No persisted sessions")
	}

	return mgr, nil
}

// Stop session manager. Stops any existing connections
func (this *Manager) Stop() error {
	select {
	case <-this.quit:
		return errors.New("already stopped")
	default:
		close(this.quit)
	}

	// stop running sessions
	this.sessions.Range(func(k, v interface{}) bool {
		wrap := v.(*container)
		wrap.rmLock.Lock()
		ses := wrap.ses
		wrap.rmLock.Unlock()

		if ses != nil {
			ses.stop(mqttp.CodeServerShuttingDown)
		} else {
			this.sessionsCount.Done()
		}

		exp := wrap.expiry.Load()
		if exp != nil {
			e := exp.(*expiry)
			if !e.cancel() {
				_ = this.persistence.ExpiryStore([]byte(k.(string)), e.persistedState())
			} else {
				this.expiryCount.Done()
			}
		}

		return true
	})

	this.sessionsCount.Wait()
	this.expiryCount.Wait()

	return nil
}

// Shutdown gracefully by stopping all active sessions and persist states
func (this *Manager) Shutdown() error {
	// shutdown subscribers
	this.sessions.Range(func(k, v interface{}) bool {
		wrap := v.(*container)
		if wrap.sub != nil {
			if err := this.persistSubscriber(wrap.sub); err != nil {
				this.log.Error("persist subscriber", zap.Error(err))
			}
		}

		this.sessions.Delete(k)

		return true
	})

	return nil
}

// GetSubscriber ...
func (this *Manager) GetSubscriber(id string) (apisubscriber.IFace, error) {
	sub, ok := this.plSubscribers[id]

	if !ok {
		sub = subscriber.NewSubscriber(subscriber.Config{
			ID: id,
			// OfflinePublish: this.pluginPublish,
		})
		this.plSubscribers[id] = sub
	}

	return sub, nil
}

// LoadSession load persisted session. Invoked by persistence provider
func (this *Manager) LoadSession(context interface{}, id []byte, state *persist.SessionState) error {
	sID := string(id)
	ctx := context.(*loadContext)

	defer func() {
		ctx.bar.IncrBy(1, time.Since(ctx.startTs))
	}()

	if len(state.Errors) != 0 {
		this.log.Error("Session load", zap.String("ClientID", sID), zap.Errors("errors", state.Errors))
		// if err := this.persistence.SubscriptionsDelete(id); err != nil && err != persistence.ErrNotFound {
		//	this.log.Error("Persisted subscriber delete", zap.Error(err))
		// }

		return nil
	}

	var err error

	status := &systree.SessionCreatedStatus{
		Clean:     false,
		Timestamp: state.Timestamp,
	}

	if err = this.decodeSessionExpiry(ctx, sID, state); err != nil {
		this.log.Error("Decode session expiry", zap.String("ClientID", sID), zap.Error(err))
	}

	if err = this.decodeSubscriber(ctx, sID, state.Subscriptions); err != nil {
		this.log.Error("Decode subscriber", zap.String("ClientID", sID), zap.Error(err))
		if err = this.persistence.SubscriptionsDelete(id); err != nil && err != persist.ErrNotFound {
			this.log.Error("Persisted subscriber delete", zap.Error(err))
		}
	}

	if cfg, ok := ctx.preloadConfigs[sID]; ok && cfg.exp != nil {
		status.WillDelay = strconv.FormatUint(uint64(cfg.exp.willIn), 10)
		if cfg.exp.expireIn != nil {
			status.ExpiryInterval = strconv.FormatUint(uint64(*cfg.exp.expireIn), 10)
		}
	}

	this.Systree.Sessions().Created(sID, status)
	return nil
}

// OnConnection implements transport.Handler interface and handles incoming connection
func (this *Manager) OnConnection(c transport.Conn, authMngr *auth.Manager) (err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			err = errors.New("panic")
		}
	}()
	var conn connection.Initial
	conn, err = connection.New(
		connection.OnAuth(this.onAuth),
		connection.NetConn(c),
		connection.TxQuota(types.DefaultReceiveMax),
		connection.RxQuota(int32(this.Options.ReceiveMax)),
		connection.Metric(this.Systree.Metric().Packets()),
		connection.RetainAvailable(this.Options.RetainAvailable),
		connection.OfflineQoS0(this.Options.OfflineQoS0),
		connection.MaxTxPacketSize(types.DefaultMaxPacketSize),
		connection.MaxRxPacketSize(this.Options.MaxPacketSize),
		connection.MaxRxTopicAlias(this.Options.MaxTopicAlias),
		connection.MaxTxTopicAlias(0),
		connection.KeepAlive(this.Options.ConnectTimeout),
		connection.Persistence(this.persistence),
	)
	if err != nil {
		return
	}

	var connParams *connection.ConnectParams
	var ack *mqttp.ConnAck
	if ch, e := conn.Accept(); e == nil {
		for dl := range ch {
			var resp mqttp.IFace
			switch obj := dl.(type) {
			case *connection.ConnectParams:
				connParams = obj
				resp, e = this.processConnect(conn, connParams, authMngr)
			case connection.AuthParams:
				resp, e = this.processAuth(connParams, obj)
			case error:
				e = obj
			default:
				e = errors.New("unknown")
			}

			if e != nil || resp == nil {
				conn.Stop(e)
				conn = nil
				return nil
			} else {
				if resp.Type() == mqttp.AUTH {
					_ = conn.Send(resp)
				} else {
					ack = resp.(*mqttp.ConnAck)
					break
				}
			}
		}
	}

	this.newSession(conn, connParams, ack, authMngr)

	return nil
}

func (this *Manager) processConnect(conn connection.Initial, params *connection.ConnectParams, authMngr *auth.Manager) (mqttp.IFace, error) {
	var resp mqttp.IFace

	if allowed, ok := this.allowedVersions[params.Version]; !ok || !allowed {
		reason := mqttp.CodeRefusedUnacceptableProtocolVersion
		if params.Version == mqttp.ProtocolV50 {
			reason = mqttp.CodeUnsupportedProtocol
		}

		return nil, reason
	}

	if len(params.AuthMethod) > 0 {
		// TODO(troian): verify method is allowed
	} else {
		var reason mqttp.ReasonCode

		if status := authMngr.Password(params.ID, string(params.Username), string(params.Password)); status == apiauth.StatusAllow {
			reason = mqttp.CodeSuccess
		} else {
			reason = mqttp.CodeRefusedBadUsernameOrPassword
			if params.Version == mqttp.ProtocolV50 {
				reason = mqttp.CodeBadUserOrPassword
			}
		}

		pkt := mqttp.NewConnAck(params.Version)
		_ = pkt.SetReturnCode(reason)
		resp = pkt
	}

	return resp, nil
}

func (this *Manager) processAuth(params *connection.ConnectParams, auth connection.AuthParams) (mqttp.IFace, error) {
	var resp mqttp.IFace

	return resp, nil
}

// newSession create new session with provided established connection
func (this *Manager) newSession(conn connection.Initial, params *connection.ConnectParams, ack *mqttp.ConnAck, authMngr *auth.Manager) {
	var ses *session
	var err error

	defer func() {
		keepAlive := int(params.KeepAlive)
		if this.KeepAlive.Force || params.KeepAlive > 0 {
			if this.KeepAlive.Force {
				keepAlive = this.KeepAlive.Period
			}
		}

		if conn.Acknowledge(ack,
			connection.KeepAlive(keepAlive),
			connection.Permissions(authMngr)) == nil {

			ses.start()

			// fixme(troian): add remote address
			status := &systree.ClientConnectStatus{
				Username:          string(params.Username),
				Timestamp:         time.Now().Format(time.RFC3339),
				ReceiveMaximum:    uint32(params.SendQuota),
				MaximumPacketSize: params.MaxTxPacketSize,
				GeneratedID:       params.IDGen,
				SessionPresent:    ack.SessionPresent(),
				KeepAlive:         uint16(keepAlive),
				ConnAckCode:       ack.ReturnCode(),
				Protocol:          params.Version,
				CleanSession:      params.CleanStart,
				Durable:           params.Durable,
			}

			this.Systree.Clients().Connected(params.ID, status)
		}
	}()

	// if response has return code differs from CodeSuccess return from this point
	// and send connack in deferred statement
	if ack.ReturnCode() != mqttp.CodeSuccess {
		return
	}

	if params.Version >= mqttp.ProtocolV50 {
		ids := ""
		if params.IDGen {
			ids = params.ID
		}

		if err = this.writeSessionProperties(ack, ids); err != nil {
			reason := mqttp.CodeUnspecifiedError
			if params.Version <= mqttp.ProtocolV50 {
				reason = mqttp.CodeRefusedServerUnavailable
			}
			_ = ack.SetReturnCode(reason)
			return
		}
	}

	var info *containerInfo
	if info, err = this.loadContainer(conn.Session(), params, authMngr); err == nil {
		ses = info.ses
		config := sessionConfig{
			sessionEvents: this,
			expireIn:      params.ExpireIn,
			will:          params.Will,
			durable:       params.Durable,
			version:       params.Version,
			subscriber:    info.sub,
		}

		_ = ses.configure(config)

		ack.SetSessionPresent(info.present)
	} else {
		var reason mqttp.ReasonCode
		if r, ok := err.(mqttp.ReasonCode); ok {
			reason = r
		} else {
			reason = mqttp.CodeUnspecifiedError
			if params.Version <= mqttp.ProtocolV50 {
				reason = mqttp.CodeRefusedServerUnavailable
			}
		}

		_ = ack.SetReturnCode(reason)
	}
}

func (this *Manager) onAuth(id string, params *connection.AuthParams) (mqttp.IFace, error) {
	return nil, nil
}

func (this *Manager) checkServerStatus(v mqttp.ProtocolVersion, resp *mqttp.ConnAck) {
	// check first if server is not about to shutdown
	// if so just give reject and exit
	select {
	case <-this.quit:
		var reason mqttp.ReasonCode
		switch v {
		case mqttp.ProtocolV50:
			reason = mqttp.CodeServerShuttingDown
			// TODO: if cluster route client to another node
		default:
			reason = mqttp.CodeRefusedServerUnavailable
		}
		if err := resp.SetReturnCode(reason); err != nil {
			this.log.Error("check server status set return code", zap.Error(err))
		}
	default:
	}
}

// allocContainer
func (this *Manager) allocContainer(id string, username string, authMngr *auth.Manager, createdAt time.Time, conn connection.Session) *container {
	ses := newSession(sessionPreConfig{
		id:          id,
		createdAt:   createdAt,
		conn:        conn,
		topicsMgr:   this.TopicsMgr,
		persistence: this.persistence,
		permissions: authMngr,
		username:    username,
	})

	cont := &container{
		removable: true,
		removed:   false,
	}

	ses.idLock = &cont.lock
	cont.ses = ses
	cont.acquire()

	return cont
}

func (this *Manager) loadContainer(conn connection.Session, params *connection.ConnectParams, authMngr *auth.Manager) (cont *containerInfo, err error) {
	newContainer := this.allocContainer(params.ID, string(params.Username), authMngr, time.Now(), conn)

	// search for existing container with given id
	if curr, present := this.sessions.LoadOrStore(params.ID, newContainer); present {
		// container with given id already exists with either active connection or expiry/willDelay set

		// release lock of newly allocated container as lock from old one will be used
		newContainer.release()

		currContainer := curr.(*container)

		// lock id to prevent other incoming connections with same ID making any changes until we done
		currContainer.acquire()
		currContainer.setRemovable(false)

		if current := currContainer.session(); current != nil {
			// container has session with active connection

			this.OnReplaceAttempt(params.ID, this.Options.SessionDups)
			if !this.Options.SessionDups {
				// we do not make any changes to current network connection
				// response to new one with error and release both new & old sessions
				err = mqttp.CodeRefusedIdentifierRejected
				if params.Version >= mqttp.ProtocolV50 {
					err = mqttp.CodeInvalidClientID
				}

				currContainer.setRemovable(true)

				currContainer.release()
				newContainer = nil
				return
			}

			// session will be replaced with new one
			// stop current active connection
			current.stop(mqttp.CodeSessionTakenOver)
		}

		// MQTT5.0 cancel expiry if set
		if val := currContainer.expiry.Load(); val != nil {
			exp := val.(*expiry)
			if exp.cancel() {
				this.expiryCount.Done()
			}

			currContainer.expiry = atomic.Value{}
		}

		currContainer.rmLock.Lock()
		removed := currContainer.removed
		currContainer.rmLock.Unlock()

		if removed {
			// if current container marked as removed check if concurrent connection has created new entry with same id
			// and reject current if so
			if _, present = this.sessions.LoadOrStore(params.ID, newContainer); present {
				err = mqttp.CodeRefusedIdentifierRejected
				if params.Version >= mqttp.ProtocolV50 {
					err = mqttp.CodeInvalidClientID
				}
				return
			} else {
				this.sessionsCount.Add(1)
			}
		} else {
			newContainer = currContainer.swap(newContainer)
			newContainer.removed = false
			newContainer.setRemovable(true)
		}
	} else {
		this.sessionsCount.Add(1)
	}

	sub := newContainer.subscriber(
		params.CleanStart,
		subscriber.Config{
			ID:             params.ID,
			OfflinePublish: this.sessionPersistPublish,
			Topics:         this.TopicsMgr,
			Version:        params.Version,
		})

	if params.CleanStart {
		if err = this.persistence.Delete([]byte(params.ID)); err != nil && err != persist.ErrNotFound {
			this.log.Error("Couldn't wipe session", zap.String("clientId", params.ID), zap.Error(err))
		} else {
			err = nil
		}
	}

	persisted := this.persistence.Exists([]byte(params.ID))

	if !persisted {
		err = this.persistence.Create([]byte(params.ID),
			&persist.SessionBase{
				Timestamp: time.Now().Format(time.RFC3339),
				Version:   byte(params.Version),
			})
		if err != nil {
			this.log.Error("Create persistence entry: ", err.Error())
		}
	}

	if err == nil {
		if !persisted {
			status := &systree.SessionCreatedStatus{
				Clean:     params.CleanStart,
				Durable:   params.Durable,
				Timestamp: time.Now().Format(time.RFC3339),
			}
			this.Systree.Sessions().Created(params.ID, status)
		}

		cont = &containerInfo{
			ses:     newContainer.ses,
			sub:     sub,
			present: persisted,
		}
	}

	return
}

func (this *Manager) writeSessionProperties(resp *mqttp.ConnAck, id string) error {
	boolToByte := func(v bool) byte {
		if v {
			return 1
		}

		return 0
	}

	// [MQTT-3.2.2.3.2] if server receive max less than 65535 than let client to know about
	if this.Options.ReceiveMax < types.DefaultReceiveMax {
		if err := resp.PropertySet(mqttp.PropertyReceiveMaximum, this.Options.ReceiveMax); err != nil {
			return err
		}
	}

	// [MQTT-3.2.2.3.3] if supported server's QoS less than 2 notify client
	if this.Options.MaxQoS < mqttp.QoS2 {
		if err := resp.PropertySet(mqttp.PropertyMaximumQoS, byte(this.Options.MaxQoS)); err != nil {
			return err
		}
	}
	// [MQTT-3.2.2.3.4] tell client whether retained messages supported
	if err := resp.PropertySet(mqttp.PropertyRetainAvailable, boolToByte(this.Options.RetainAvailable)); err != nil {
		return err
	}
	// [MQTT-3.2.2.3.5] if server max packet size less than 268435455 than let client to know about
	if this.Options.MaxPacketSize < types.DefaultMaxPacketSize {
		if err := resp.PropertySet(mqttp.PropertyMaximumPacketSize, this.Options.MaxPacketSize); err != nil {
			return err
		}
	}
	// [MQTT-3.2.2.3.6]
	if len(id) > 0 {
		if err := resp.PropertySet(mqttp.PropertyAssignedClientIdentifier, id); err != nil {
			return err
		}
	}
	// [MQTT-3.2.2.3.7]
	if this.Options.MaxTopicAlias > 0 {
		if err := resp.PropertySet(mqttp.PropertyTopicAliasMaximum, this.Options.MaxTopicAlias); err != nil {
			return err
		}
	}
	// [MQTT-3.2.2.3.10] tell client whether server supports wildcard subscriptions or not
	if err := resp.PropertySet(mqttp.PropertyWildcardSubscriptionAvailable, boolToByte(this.Options.SubsWildcard)); err != nil {
		return err
	}
	// [MQTT-3.2.2.3.11] tell client whether server supports subscription identifiers or not
	if err := resp.PropertySet(mqttp.PropertySubscriptionIdentifierAvailable, boolToByte(this.Options.SubsID)); err != nil {
		return err
	}
	// [MQTT-3.2.2.3.12] tell client whether server supports shared subscriptions or not
	if err := resp.PropertySet(mqttp.PropertySharedSubscriptionAvailable, boolToByte(this.Options.SubsShared)); err != nil {
		return err
	}

	if this.KeepAlive.Force {
		if err := resp.PropertySet(mqttp.PropertyServerKeepAlive, uint16(this.KeepAlive.Period)); err != nil {
			return err
		}
	}

	return nil
}

func (this *Manager) connectionClosed(id string, reason mqttp.ReasonCode) {
	this.Systree.Clients().Disconnected(id, reason)
}

func (this *Manager) subscriberShutdown(id string, sub apisubscriber.IFace) {
	sub.Offline(true)
	if val, ok := this.sessions.Load(id); ok {
		wrap := val.(*container)
		wrap.sub = nil
	} else {
		this.log.Error("subscriber shutdown. container not found", zap.String("ClientID", id))
	}
}

func (this *Manager) sessionOffline(id string, keep bool, expCfg *expiryConfig) {
	if obj, ok := this.sessions.Load(id); ok {
		if cont, kk := obj.(*container); kk {
			cont.rmLock.Lock()
			cont.ses = nil

			if keep {
				if expCfg != nil {
					expCfg.expiryEvent = this
					exp := newExpiry(*expCfg)
					cont.expiry.Store(exp)

					this.expiryCount.Add(1)
					exp.start()
				}
			} else {
				if cont.removable {
					state := &systree.SessionDeletedStatus{
						Timestamp: time.Now().Format(time.RFC3339),
						Reason:    "shutdown",
					}

					this.Systree.Sessions().Removed(id, state)
					this.sessions.Delete(id)
					this.sessionsCount.Done()
					_ = this.persistence.Delete([]byte(id))
					cont.removed = true
				}
			}
			cont.rmLock.Unlock()
		} else {
			this.log.Panic("is not a container")
		}
	} else {
		this.log.Error("Couldn't wipe session, object does not exist")
	}
}

func (this *Manager) sessionTimer(id string, expired bool) {
	rs := "shutdown"
	if expired {
		rs = "expired"

		_ = this.persistence.Delete([]byte(id))

		this.sessions.Delete(id)
		this.sessionsCount.Done()
	}

	state := &systree.SessionDeletedStatus{
		Timestamp: time.Now().Format(time.RFC3339),
		Reason:    rs,
	}

	this.Systree.Sessions().Removed(id, state)

	if expired {
		this.expiryCount.Done()
	}
}

func (this *Manager) configurePersistedSubscribers(ctx *loadContext) {
	for id, t := range ctx.preloadConfigs {
		sub := subscriber.NewSubscriber(
			subscriber.Config{
				ID:             id,
				Topics:         this.TopicsMgr,
				OfflinePublish: this.sessionPersistPublish,
				Version:        t.sub.version,
			})

		for topic, ops := range t.sub.topics {
			if _, err := sub.Subscribe(topic, ops); err != nil {
				this.log.Error("Couldn't subscribe", zap.Error(err))
			}
		}

		cont := &container{
			removable: true,
			removed:   false,
			sub:       sub,
		}

		this.sessions.Store(id, cont)
		this.sessionsCount.Add(1)
	}
}

func (this *Manager) configurePersistedExpiry(ctx *loadContext) {
	for id, t := range ctx.preloadConfigs {
		cont := &container{
			removable: true,
			removed:   false,
		}

		this.expiryCount.Add(1)

		exp := newExpiry(*t.exp)

		cont.expiry.Store(exp)
		if c, present := this.sessions.LoadOrStore(id, cont); present {
			cnt := c.(*container)
			cnt.expiry.Store(exp)
		}

		exp.start()
	}
}

func (this *Manager) processDelayedWills(ctx *loadContext) {
	for _, will := range ctx.delayedWills {
		if err := this.TopicsMgr.Publish(will); err != nil {
			this.log.Error("Publish delayed will", zap.Error(err))
		}
	}
}

// decodeSessionExpiry
func (this *Manager) decodeSessionExpiry(ctx *loadContext, id string, state *persist.SessionState) error {
	if state.Expire == nil {
		return nil
	}

	var err error
	var since time.Time

	if len(state.Expire.Since) > 0 {
		since, err = time.Parse(time.RFC3339, state.Expire.Since)
		if err != nil {
			this.log.Error("parse expiration value", zap.String("clientId", id), zap.Error(err))
			if e := this.persistence.SubscriptionsDelete([]byte(id)); e != nil && e != persist.ErrNotFound {
				this.log.Error("Persisted subscriber delete", zap.Error(e))
			}

			return err
		}
	}
	var will *mqttp.Publish
	var willIn uint32
	var expireIn uint32

	// if persisted state has delayed will lets check if it has not elapsed its time
	if len(state.Expire.Will) > 0 {
		pkt, _, _ := mqttp.Decode(mqttp.ProtocolV50, state.Expire.Will)
		will, _ = pkt.(*mqttp.Publish)

		if prop := pkt.PropertyGet(mqttp.PropertyWillDelayInterval); prop != nil {
			willIn, _ = prop.AsInt()
			willAt := since.Add(time.Duration(willIn) * time.Second)
			if time.Now().After(willAt) {
				// will delay elapsed. notify keep in list and publish when all persisted sessions loaded
				ctx.delayedWills = append(ctx.delayedWills, will)
				will = nil
				willIn = 0
			}
		}
	}

	if len(state.Expire.ExpireIn) > 0 {
		var val int
		if val, err = strconv.Atoi(state.Expire.ExpireIn); err == nil {
			expireIn = uint32(val)
			expireAt := since.Add(time.Duration(expireIn) * time.Second)

			if time.Now().After(expireAt) {
				// persisted session has expired, wipe it
				if err = this.persistence.Delete([]byte(id)); err != nil && err != persist.ErrNotFound {
					this.log.Error("Delete expired session", zap.Error(err))
				}
				return nil
			}
		} else {
			this.log.Error("Decode expire at", zap.String("clientId", id), zap.Error(err))
		}
	}

	// persisted session has either delayed will or expiry
	// create it and run timer
	if will != nil || expireIn > 0 {
		var createdAt time.Time
		if createdAt, err = time.Parse(time.RFC3339, state.Timestamp); err != nil {
			this.log.Named("persistence").Error("Decode createdAt failed, using current timestamp",
				zap.String("clientId", id),
				zap.Error(err))
			createdAt = time.Now()
		}

		if _, ok := ctx.preloadConfigs[id]; !ok {
			ctx.preloadConfigs[id] = &preloadConfig{}
		}

		var expiringSince time.Time

		if expireIn > 0 {
			if expiringSince, err = time.Parse(time.RFC3339, state.Expire.Since); err != nil {
				this.log.Named("persistence").Error("Decode Expire.Since failed",
					zap.String("clientId", id),
					zap.Error(err))
			}
		}

		ctx.preloadConfigs[id].exp = &expiryConfig{
			expiryEvent:   this,
			topicsMgr:     this.TopicsMgr,
			createdAt:     createdAt,
			expiringSince: expiringSince,
			will:          will,
			willIn:        willIn,
			expireIn:      &expireIn,
		}
	}

	return nil
}

// decodeSubscriber function invoke only during server startup. Used to decode persisted session
// which has active subscriptions
func (this *Manager) decodeSubscriber(ctx *loadContext, id string, from []byte) error {
	if len(from) == 0 {
		return nil
	}

	subscriptions := apisubscriber.Subscriptions{}
	offset := 0
	version := mqttp.ProtocolVersion(from[offset])
	offset++
	remaining := len(from) - 1
	for offset != remaining {
		t, total, e := mqttp.ReadLPBytes(from[offset:])
		if e != nil {
			return e
		}

		offset += total

		params := &apisubscriber.SubscriptionParams{}

		params.Ops = mqttp.SubscriptionOptions(from[offset])
		offset++

		params.ID = binary.BigEndian.Uint32(from[offset:])
		offset += 4
		subscriptions[string(t)] = params
	}

	if _, ok := ctx.preloadConfigs[id]; !ok {
		ctx.preloadConfigs[id] = &preloadConfig{}
	}

	ctx.preloadConfigs[id].sub = &subscriberConfig{
		version: version,
		topics:  subscriptions,
	}

	return nil
}

func (this *Manager) persistSubscriber(s *subscriber.Subscriber) error {
	topics := s.Subscriptions()

	// calculate size of the encoded entry
	// consist of:
	//  _ _ _ _ _     _ _ _ _ _ _
	// |_|_|_|_|_|...|_|_|_|_|_|_|
	//  ___ _ _________ _ _______
	//   |  |     |     |    |
	//   |  |     |     |    4 bytes - subscription id
	//   |  |     |     | 1 byte - topic options
	//   |  |     | n bytes - topic
	//   |  | 1 bytes - protocol version
	//   | 2 bytes - length prefix

	size := 0
	for topic := range topics {
		size += 2 + len(topic) + 1 + int(unsafe.Sizeof(uint32(0)))
	}

	buf := make([]byte, size+1)
	offset := 0
	buf[offset] = byte(s.GetVersion())
	offset++

	for topic, params := range topics {
		total, _ := mqttp.WriteLPBytes(buf[offset:], []byte(topic))
		offset += total
		buf[offset] = byte(params.Ops)
		offset++
		binary.BigEndian.PutUint32(buf[offset:], params.ID)
		offset += 4
	}

	if err := this.persistence.SubscriptionsStore([]byte(s.ID), buf); err != nil {
		this.log.Error("Couldn't persist subscriptions", zap.String("ClientID", s.ID), zap.Error(err))
	}

	s.Offline(true)
	return nil
}

func (this *Manager) sessionPersistPublish(id string, p *mqttp.Publish) {
	pkt := &persist.PersistedPacket{}

	var expired bool
	var expireAt time.Time

	if expireAt, _, expired = p.Expired(); expired {
		return
	}

	if !expireAt.IsZero() {
		pkt.ExpireAt = expireAt.Format(time.RFC3339)
	}

	p.SetPacketID(0)

	var err error
	pkt.Data, err = mqttp.Encode(p)
	if err != nil {
		this.log.Error("Couldn't encode packet", zap.String("ClientID", id), zap.Error(err))
		return
	}

	if p.QoS() == mqttp.QoS0 {
		err = this.persistence.PacketStoreQoS0([]byte(id), pkt)
	} else {
		err = this.persistence.PacketStoreQoS12([]byte(id), pkt)
	}

	if err != nil {
		this.log.Error("Couldn't persist message", zap.String("ClientID", id), zap.Error(err))
	}
}
