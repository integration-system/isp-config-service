package service

import (
	"encoding/json"
	"github.com/cenkalti/backoff"
	"github.com/integration-system/isp-lib/logger"
	"github.com/integration-system/isp-lib/structure"
	"github.com/integration-system/isp-lib/utils"
	"isp-config-service/holder"
	"isp-config-service/store/state"
	"isp-config-service/ws"
	"sync"
	"time"
)

const (
	RoutesSubscribersRoom = "__routesSubscribers"
)

var (
	DiscoveryService = NewDiscoveryService()
)

type discoveryService struct {
	subs map[string][]string
	lock sync.RWMutex
}

func (ds *discoveryService) HandleDisconnect(connId string) {
	ds.lock.Lock()
	defer ds.lock.Unlock()
	if events, ok := ds.subs[connId]; ok {
		holder.Socket.Rooms().LeaveByConnId(connId, events...)
		delete(ds.subs, connId)
	}
	holder.Socket.Rooms().LeaveByConnId(connId, RoutesSubscribersRoom)
}

func (ds *discoveryService) Subscribe(conn ws.Conn, events []string, state state.ReadState) {
	if len(events) == 0 {
		return
	}
	ds.lock.Lock()
	defer ds.lock.Unlock()
	ds.subs[conn.Id()] = events
	holder.Socket.Rooms().Join(conn, events...)
	for _, event := range events {
		addressList := state.GetModuleAddresses(event)
		event = utils.ModuleConnected(event)
		ds.sendAddrList(conn, event, addressList)
	}
}

func (ds *discoveryService) BroadcastModuleAddresses(moduleName string, state state.State) {
	ds.lock.RLock()
	defer ds.lock.RUnlock()
	event := utils.ModuleConnected(moduleName)
	addressList := state.GetModuleAddresses(moduleName)
	ds.broadcastAddrList(moduleName, event, addressList)
}

func (ds *discoveryService) broadcastAddrList(moduleName string, event string, addressList []structure.AddressConfiguration) {
	if bytes, err := json.Marshal(addressList); err != nil {
		logger.Warn(err)
	} else {
		err = holder.Socket.Broadcast(moduleName, event, string(bytes))
		if err != nil {
			logger.Error(err)
		}
	}
}

func (ds *discoveryService) sendAddrList(conn ws.Conn, event string, addressList []structure.AddressConfiguration) {
	if bytes, err := json.Marshal(addressList); err != nil {
		logger.Warn(err)
	} else {
		bf := backoff.WithMaxRetries(backoff.NewConstantBackOff(100*time.Millisecond), 3)
		err := backoff.Retry(func() error {
			return conn.Emit(event, string(bytes))
		}, bf)
		if err != nil {
			logger.Error(err)
		}
	}
}

func NewDiscoveryService() *discoveryService {
	return &discoveryService{
		subs: make(map[string][]string),
	}
}