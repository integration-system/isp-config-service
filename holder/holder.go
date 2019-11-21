package holder

import (
	etp "github.com/integration-system/isp-etp-go"
	"isp-config-service/cluster"
	"net/http"
)

var (
	ClusterClient *cluster.Client
	EtpServer     etp.Server
	HttpServer    *http.Server
)
