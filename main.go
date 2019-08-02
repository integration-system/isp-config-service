package main

import (
	"context"
	"github.com/integration-system/isp-lib/backend"
	"github.com/integration-system/isp-lib/config"
	"github.com/integration-system/isp-lib/logger"
	"github.com/integration-system/isp-lib/structure"
	"github.com/thecodeteam/goodbye"
	"isp-config-service/cluster"
	"isp-config-service/conf"
	"isp-config-service/helper"
	"isp-config-service/holder"
	"isp-config-service/raft"
	"isp-config-service/state"
	"isp-config-service/subs"
	"isp-config-service/ws"
	"net/http"
	"os"
)

const (
	moduleName = "config"
)

var (
	version = "0.1.0"
	date    = "undefined"

	shutdownChan = make(chan struct{})
)

func init() {
	config.InitConfig(&conf.Configuration{})
}

func main() {
	cfg := config.Get().(*conf.Configuration)

	client, store := initRaft(cfg.WS.Raft.GetAddress(), cfg.Cluster)
	initWebsocket(cfg.WS.Rest.GetAddress(), client, store)
	initGrpc(cfg.WS.Grpc)

	ctx := context.Background()
	defer goodbye.Exit(ctx, -1)
	goodbye.Notify(ctx)
	goodbye.Register(onShutdown)

	<-shutdownChan
}

func initWebsocket(bindAddress string, clusterClint *cluster.ClusterClient, store *state.Store) {
	socket, err := ws.NewWebsocketServer()
	if err != nil {
		logger.Fatal(err)
	}
	subs.NewSocketEventHandler(socket, clusterClint, store).SubscribeAll()

	mux := http.NewServeMux()
	mux.HandleFunc("/socket.io/", socket.ServeHttp)
	httpServer := &http.Server{Addr: bindAddress, Handler: mux}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			logger.Fatal(err)
		}
	}()
	holder.Socket = socket
	holder.HttpServer = httpServer

	logger.Infof("socket.IO server start on %s", bindAddress)
}

func initRaft(bindAddress string, clusterCfg conf.ClusterConfiguration) (*cluster.ClusterClient, *state.Store) {
	store := state.NewStateStore()
	r, err := raft.NewRaft(bindAddress, clusterCfg, store)
	if err != nil {
		logger.Fatal(err)
	}
	clusterClient := cluster.NewRaftClusterClient(r)
	holder.ClusterClient = clusterClient

	_ = r.BootstrapCluster() // err can be ignored

	logger.Infof("raft server start on %s", bindAddress)

	return clusterClient, store
}

func initGrpc(bindAddress structure.AddressConfiguration) {
	service := backend.GetDefaultService(moduleName, helper.GetAllHandlers()...)
	backend.StartBackendGrpcServer(bindAddress, service)
}

func onShutdown(ctx context.Context, sig os.Signal) {
	defer close(shutdownChan)

	backend.StopGrpcServer()

	if err := holder.ClusterClient.Shutdown(); err != nil {
		logger.Warnf("raft shutdown err: %v", err)
	} else {
		logger.Info("raft shutdown success")
	}

	if err := holder.Socket.Close(); err != nil {
		logger.Info("socket.io shutdown err: %v", err)
	} else {
		logger.Info("socket.io shutdown success")
	}

	if err := holder.HttpServer.Shutdown(ctx); err != nil {
		logger.Info("http server shutdown err: %v", err)
	} else {
		logger.Info("http server shutdown success")
	}
}
