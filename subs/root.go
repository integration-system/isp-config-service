package subs

import (
	etp "github.com/integration-system/isp-etp-go"
	"github.com/integration-system/isp-lib/utils"
	log "github.com/integration-system/isp-log"
	jsoniter "github.com/json-iterator/go"
	"isp-config-service/cluster"
	"isp-config-service/codes"
	"isp-config-service/entity"
	"isp-config-service/holder"
	"isp-config-service/service"
	"isp-config-service/store"
	"isp-config-service/store/state"
)

const (
	followersRoom = "cluster_followers"
)

var (
	json = jsoniter.ConfigFastest
)

type socketEventHandler struct {
	server etp.Server
	store  *store.Store
}

func (h *socketEventHandler) SubscribeAll() {
	h.server.
		OnConnect(h.handleConnect).
		OnDisconnect(h.handleDisconnect).
		OnError(h.handleError).
		OnWithAck(cluster.ApplyCommandEvent, h.applyCommandOnLeader).
		OnWithAck(utils.ModuleReady, h.handleModuleReady).
		OnWithAck(utils.ModuleSendRequirements, h.handleModuleRequirements).
		OnWithAck(utils.ModuleSendConfigSchema, h.handleConfigSchema)
}

func (h *socketEventHandler) handleConnect(conn etp.Conn) {
	isClusterNode := IsConfigClusterNode(conn)
	if isClusterNode {
		holder.EtpServer.Rooms().Join(conn, followersRoom)
	}
	moduleName, err := Parameters(conn)
	log.Debugf(0, "handleConnect: %s", moduleName) // REMOVE
	if err != nil {
		EmitConn(conn, utils.ErrorConnection, FormatErrorConnection(err))
		_ = conn.Close()
		return
	}
	holder.EtpServer.Rooms().Join(conn, moduleName+service.ConfigWatchersRoomSuffix)
	now := state.GenerateDate()
	module := entity.Module{
		Id:              state.GenerateId(),
		Name:            moduleName,
		CreatedAt:       now,
		LastConnectedAt: now,
	}
	command := cluster.PrepareModuleConnectedCommand(module)
	_, err = SyncApplyCommand(command, "ModuleConnectedCommand")
	if err != nil && !isClusterNode {
		EmitConn(conn, utils.ErrorConnection, FormatErrorConnection(err))
		_ = conn.Close()
		return
	}

	if isClusterNode {
		return
	}

	var config map[string]interface{}
	h.store.VisitReadonlyState(func(state state.ReadonlyState) {
		config, err = service.ConfigService.GetCompiledConfig(moduleName, state)
	})
	if err != nil {
		EmitConn(conn, utils.ConfigError, []byte(err.Error()))
		return
	}
	data, err := json.Marshal(config)
	if err != nil {
		EmitConn(conn, utils.ConfigError, []byte(err.Error()))
		return
	}
	EmitConn(conn, utils.ConfigSendConfigWhenConnected, data)
}

func (h *socketEventHandler) handleDisconnect(conn etp.Conn, disconnectErr error) {
	if IsConfigClusterNode(conn) {
		holder.EtpServer.Rooms().Leave(conn, followersRoom)
	}
	moduleName, _ := Parameters(conn)
	log.Debugf(0, "handleDisconnect: %s", moduleName) // REMOVE
	if moduleName != "" {
		holder.EtpServer.Rooms().Leave(conn, moduleName+service.ConfigWatchersRoomSuffix)
		now := state.GenerateDate()
		module := entity.Module{
			Id:                 state.GenerateId(),
			Name:               moduleName,
			CreatedAt:          now,
			LastConnectedAt:    now,
			LastDisconnectedAt: now,
		}
		command := cluster.PrepareModuleDisconnectedCommand(module)
		_, _ = SyncApplyCommand(command, "ModuleDisconnectedCommand")
	}

	service.DiscoveryService.HandleDisconnect(conn.ID())
	service.RoutesService.HandleDisconnect(conn.ID())
	backend, ok := ExtractBackendDeclaration(conn)
	if ok {
		command := cluster.PrepareDeleteBackendDeclarationCommand(backend)
		_, _ = SyncApplyCommand(command, "DeleteBackendDeclarationCommand")
	}
}

func (h *socketEventHandler) handleError(conn etp.Conn, err error) {
	log.Warnf(codes.WebsocketError, "isp-etp: %v", err)
}

func NewSocketEventHandler(server etp.Server, store *store.Store) *socketEventHandler {
	return &socketEventHandler{
		server: server,
		store:  store,
	}
}
