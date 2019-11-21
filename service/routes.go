package service

import (
	"context"
	"encoding/json"
	"github.com/cenkalti/backoff"
	etp "github.com/integration-system/isp-etp-go"
	"github.com/integration-system/isp-lib/structure"
	"github.com/integration-system/isp-lib/utils"
	log "github.com/integration-system/isp-log"
	"isp-config-service/codes"
	"isp-config-service/holder"
	"isp-config-service/store/state"
	"time"
)

const (
	RoutesSubscribersRoom = "__routesSubscribers"
)

var RoutesService routesService

type routesService struct{}

func (rs *routesService) HandleDisconnect(connId string) {
	holder.EtpServer.Rooms().LeaveByConnId(connId, RoutesSubscribersRoom)
}

func (rs *routesService) SubscribeRoutes(conn etp.Conn, mesh state.ReadonlyMesh) {
	holder.EtpServer.Rooms().Join(conn, RoutesSubscribersRoom)
	routes := mesh.GetRoutes()
	go func(conn etp.Conn, routes structure.RoutingConfig) {
		err := rs.sendRoutes(conn, utils.ConfigSendRoutesWhenConnected, routes)
		if err != nil {
			log.Errorf(codes.RoutesServiceSendRoutesError, "send routes %v", err)
		}
	}(conn, routes)
}

func (rs *routesService) BroadcastRoutes(mesh state.ReadonlyMesh) {
	routes := mesh.GetRoutes()
	go func(routes structure.RoutingConfig) {
		err := rs.broadcastRoutes(utils.ConfigSendRoutesChanged, routes)
		if err != nil {
			log.Errorf(codes.RoutesServiceSendRoutesError, "broadcast routes %v", err)
		}
	}(routes)
}

func (rs *routesService) broadcastRoutes(event string, routes structure.RoutingConfig) error {
	if bytes, err := json.Marshal(routes); err != nil {
		return err
	} else {
		err = holder.EtpServer.BroadcastToRoom(RoutesSubscribersRoom, event, bytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rs *routesService) sendRoutes(conn etp.Conn, event string, routes structure.RoutingConfig) error {
	if bytes, err := json.Marshal(routes); err != nil {
		return err
	} else {
		bf := backoff.WithMaxRetries(backoff.NewConstantBackOff(100*time.Millisecond), 3)
		err := backoff.Retry(func() error {
			return conn.Emit(context.Background(), event, bytes)
		}, bf)
		if err != nil {
			return err
		}
	}
	return nil
}
