package cluster

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff"
	etp "github.com/integration-system/isp-etp-go/client"
	"github.com/integration-system/isp-lib/config"
	"github.com/integration-system/isp-lib/structure"
	"github.com/integration-system/isp-lib/utils"
	log "github.com/integration-system/isp-log"
	"isp-config-service/codes"
	"isp-config-service/conf"
	"net"
	"net/http"
	"net/url"
	"time"
)

type SocketLeaderClient struct {
	client    etp.Client
	url       string
	globalCtx context.Context
	cancel    context.CancelFunc
}

func (c *SocketLeaderClient) Ack(data []byte, timeout time.Duration) ([]byte, error) {
	ctx, _ := context.WithTimeout(c.globalCtx, timeout)
	response, err := c.client.EmitWithAck(ctx, ApplyCommandEvent, data)
	return response, err
}

func (c *SocketLeaderClient) SendDeclaration(backend structure.BackendDeclaration, timeout time.Duration) (string, error) {
	data, err := json.Marshal(backend)
	if err != nil {
		return "", err
	}
	ctx, _ := context.WithTimeout(c.globalCtx, timeout)
	response, err := c.client.EmitWithAck(ctx, utils.ModuleReady, data)
	return string(response), err
}

func (c *SocketLeaderClient) Dial(timeout time.Duration) error {
	backOff := backoff.NewExponentialBackOff()
	backOff.MaxElapsedTime = timeout
	bf := backoff.WithContext(backOff, c.globalCtx)
	dial := func() error {
		return c.client.Dial(c.globalCtx, c.url)
	}
	return backoff.Retry(dial, bf)
}

func (c *SocketLeaderClient) Close() {
	c.cancel()
	err := c.client.Close()
	if err != nil {
		log.Warnf(codes.LeaderClientConnectionError, "leader client close err: %v", err)
	}
	log.Debug(0, "leader client connection closed")
}

func NewSocketLeaderClient(address string, leaderDisconnectionCallback func()) *SocketLeaderClient {
	etpConfig := etp.Config{
		HttpClient: http.DefaultClient,
	}
	client := etp.NewClient(etpConfig)
	leaderClient := &SocketLeaderClient{
		client: client,
		url:    getUrl(address),
	}
	ctx, cancel := context.WithCancel(context.Background())
	leaderClient.globalCtx = ctx
	leaderClient.cancel = cancel

	leaderClient.client.OnDisconnect(func(err error) {
		log.WithMetadata(map[string]interface{}{
			"leaderAddr": address,
		}).Warn(codes.LeaderClientDisconnected, "leader client disconnected")
		leaderDisconnectionCallback()
	})

	leaderClient.client.OnError(func(err error) {
		log.Warnf(codes.LeaderClientConnectionError, "leader client on error: %v", err)
	})
	leaderClient.client.OnConnect(func() {
		log.Debug(0, "leader client connected")
	})
	return leaderClient
}

func getUrl(address string) string {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		panic(err) // must never occurred
	}
	cfg := config.Get().(*conf.Configuration)

	// REMOVE
	addr.Port = addr.Port + 5

	params := url.Values{}
	params.Add(ClusterParam, "true")
	// TODO выпилить instance_uuid ?
	params.Add(utils.ModuleNameGetParamKey, cfg.ModuleName)
	params.Add(utils.InstanceUuidGetParamKey, "9d89354b-c728-4b48-b002-a7d3b229f151")
	return fmt.Sprintf("ws://%s:%d/isp-etp/?%s", addr.IP.String(), addr.Port, params.Encode())
}
