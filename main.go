package main

import (
	"context"
	"fmt"
	"github.com/integration-system/isp-etp-go"
	"github.com/integration-system/isp-lib/backend"
	"github.com/integration-system/isp-lib/bootstrap"
	"github.com/integration-system/isp-lib/config"
	"github.com/integration-system/isp-lib/structure"
	"github.com/integration-system/isp-lib/utils"
	log "github.com/integration-system/isp-log"
	"github.com/thecodeteam/goodbye"
	"isp-config-service/cluster"
	"isp-config-service/codes"
	"isp-config-service/conf"
	"isp-config-service/controller"
	"isp-config-service/helper"
	"isp-config-service/holder"
	"isp-config-service/model"
	"isp-config-service/raft"
	"isp-config-service/service"
	"isp-config-service/store"
	"isp-config-service/store/state"
	"isp-config-service/subs"
	"net"
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
	if utils.LOG_LEVEL != "" {
		_ = log.SetLevel(utils.LOG_LEVEL)
	}
}

// @title ISP configuration service
// @version 2.0.0
// @description Сервис управления конфигурацией модулей ISP кластера

// @license.name GNU GPL v3.0

// @host localhost:9003
// @BasePath /api/config
func main() {
	ctx := context.Background()
	cfg := config.Get().(*conf.Configuration)
	handlers := helper.GetHandlers()
	endpoints := backend.GetEndpoints(cfg.ModuleName, handlers)
	address := cfg.GrpcOuterAddress
	if address.IP == "" {
		ip, err := getOutboundIp()
		if err != nil {
			panic(err)
		}
		address.IP = ip
	}
	declaration := structure.BackendDeclaration{
		ModuleName: cfg.ModuleName,
		Version:    version,
		LibVersion: bootstrap.LibraryVersion,
		Endpoints:  endpoints,
		Address:    address,
	}

	model.DbClient.ReceiveConfiguration(cfg.Database)

	httpListener, raftListener, err := initMultiplexer(cfg.WS.Rest)
	if err != nil {
		log.Fatalf(codes.InitCmuxError, "init cmux: %v", err)
	}

	_, raftStore := initRaft(raftListener, cfg.Cluster, declaration)
	initWebsocket(ctx, httpListener, raftStore)
	initGrpc(cfg.WS.Grpc, raftStore)

	defer goodbye.Exit(ctx, -1)
	goodbye.Notify(ctx)
	goodbye.Register(onShutdown)

	<-shutdownChan
}

func initMultiplexer(addressConfiguration structure.AddressConfiguration) (net.Listener, net.Listener, error) {
	outerAddr, err := net.ResolveTCPAddr("tcp4", addressConfiguration.GetAddress())
	if err != nil {
		return nil, nil, fmt.Errorf("resolve outer address: %v", err)
	}
	tcpListener, err := net.ListenTCP("tcp4", outerAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("create tcp transport: %v", err)
	}

	// REMOVE
	outerAddr.Port = outerAddr.Port + 5
	httpListener, err := net.ListenTCP("tcp4", outerAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("create tcp transport: %v", err)
	}
	return httpListener, tcpListener, nil

	//m := cmux.New(tcpListener)
	//httpListener := m.Match(cmux.HTTP1Fast())
	//raftListener := m.Match(cmux.Any())
	//
	//go func() {
	//	if err := m.Serve(); err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
	//		log.Fatalf(codes.InitCmuxError, "serve cmux: %v", err)
	//	}
	//}()
	//return httpListener, raftListener, nil
}

func initWebsocket(ctx context.Context, listener net.Listener, raftStore *store.Store) {
	etpConfig := etp.ServerConfig{
		InsecureSkipVerify: true,
	}
	etpServer := etp.NewServer(ctx, etpConfig)
	subs.NewSocketEventHandler(etpServer, raftStore).SubscribeAll()

	mux := http.NewServeMux()
	mux.HandleFunc("/isp-etp/", etpServer.ServeHttp)
	httpServer := &http.Server{Handler: mux}
	go func() {
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Fatalf(codes.StartHttpServerError, "http server closed: %v", err)
		}
	}()
	holder.EtpServer = etpServer
	holder.HttpServer = httpServer
}

func initRaft(listener net.Listener, clusterCfg conf.ClusterConfiguration, declaration structure.BackendDeclaration) (*cluster.Client, *store.Store) {
	raftState, err := store.NewStateFromRepository()
	if err != nil {
		log.Fatal(codes.RestoreFromRepositoryError, err)
		return nil, nil
	}
	raftStore := store.NewStateStore(raftState)
	r, err := raft.NewRaft(listener, clusterCfg, raftStore)
	if err != nil {
		log.Fatalf(codes.InitRaftError, "unable to create raft server. %v", err)
		return nil, nil
	}
	clusterClient := cluster.NewRaftClusterClient(r, declaration, func(address string) {
		raftStore.VisitState(func(s state.WritableState) {
			cfg := config.Get().(*conf.Configuration)
			addr, err := net.ResolveTCPAddr("tcp", address)
			if err != nil {
				panic(err) // must never occured
			}
			port := cfg.GrpcOuterAddress.Port
			addressConfiguration := structure.AddressConfiguration{Port: port, IP: addr.IP.String()}
			back := structure.BackendDeclaration{ModuleName: cfg.ModuleName, Address: addressConfiguration}
			log.WithMetadata(map[string]interface{}{"declaration": back}).
				Debugf(0, "delete disconnected leader's declaration")
			service.ClusterMeshService.HandleDeleteBackendDeclarationCommand(back, s)
		})
	})
	holder.ClusterClient = clusterClient

	if clusterCfg.BootstrapCluster {
		err = r.BootstrapCluster() // err can be ignored
		if err != nil {
			log.Errorf(codes.BootstrapClusterError, "bootstrap cluster. %v", err)
		}
	}
	return clusterClient, raftStore
}

func initGrpc(bindAddress structure.AddressConfiguration, raftStore *store.Store) {
	controller.Routes = controller.NewRoutes(raftStore)
	controller.Schema = controller.NewSchema(raftStore)
	controller.Module = controller.NewModule(raftStore)
	controller.Config = controller.NewConfig(raftStore)
	controller.CommonConfig = controller.NewCommonConfig(raftStore)
	defaultService := backend.GetDefaultService(moduleName, helper.GetHandlers())
	backend.StartBackendGrpcServer(bindAddress, defaultService)
}

func onShutdown(ctx context.Context, sig os.Signal) {
	log.Debugf(0, "received shutdown signal: %s", sig.String())
	defer close(shutdownChan)

	backend.StopGrpcServer()
	holder.EtpServer.Close()

	if err := holder.ClusterClient.Shutdown(); err != nil {
		log.Warnf(codes.RaftShutdownError, "raft shutdown err: %v", err)
	} else {
		log.Info(codes.RaftShutdownInfo, "raft shutdown success")
	}

	if err := holder.HttpServer.Shutdown(ctx); err != nil {
		log.Warnf(codes.ShutdownHttpServerError, "http server shutdown err: %v", err)
	} else {
		log.Info(codes.ShutdownHttpServerInfo, "http server shutdown success")
	}
}

func getOutboundIp() (string, error) {
	conn, err := net.Dial("udp", "9.9.9.9:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.To4().String(), nil
}
