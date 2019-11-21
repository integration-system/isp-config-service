package subs

import (
	etp "github.com/integration-system/isp-etp-go"
	"github.com/integration-system/isp-lib/structure"
	"github.com/integration-system/isp-lib/utils"
	"isp-config-service/cluster"
)

type connectionState struct {
	declaration structure.BackendDeclaration
}

func IsConfigClusterNode(conn etp.Conn) bool {
	isCluster := conn.URL().Query().Get(cluster.ClusterParam)
	return isCluster == "true"

}

func Parameters(conn etp.Conn) (moduleName string, err error) {
	// TODO remove instanceUUID from isp-lib
	_, moduleName, err = utils.ParseParameters(conn.URL().RawQuery)
	return moduleName, err
}

func SetBackendDeclaration(conn etp.Conn, backend structure.BackendDeclaration) {
	conn.SetContext(connectionState{declaration: backend})
}

func ExtractBackendDeclaration(conn etp.Conn) (structure.BackendDeclaration, bool) {
	declaration, ok := conn.Context().(connectionState)
	return declaration.declaration, ok
}
