package domain

import (
	"time"

	"github.com/integration-system/isp-lib/v2/config/schema"
	"github.com/integration-system/isp-lib/v2/structure"
	"isp-config-service/entity"
)

type DeleteResponse struct {
	Deleted int
}

type ModuleInfo struct {
	Id                        string
	Name                      string
	Active                    bool
	CreatedAt                 time.Time
	LastConnectedAt           time.Time
	LastDisconnectedAt        time.Time
	Configs                   []ConfigModuleInfo
	ConfigSchema              *schema.Schema
	Status                    []Connection
	RequiredModules           []ModuleDependency
	ProtocolPathsDiscriptions []ProtocolPathsDiscription
}

type ProtocolPathsDiscription struct {
	Protocol      string
	Address       structure.AddressConfiguration
	HandlersPaths []string
}

type Connection struct {
	LibVersion    string
	Version       string
	Address       structure.AddressConfiguration
	Endpoints     []structure.EndpointDescriptor
	EstablishedAt time.Time
}

type ModuleDependency struct {
	Id       string
	Name     string
	Required bool
}

type CommonConfigLinks map[string][]string

type CompiledConfigResponse map[string]interface{}

type DeleteCommonConfigResponse struct {
	Deleted bool
	Links   CommonConfigLinks
}

type CreateUpdateConfigResponse struct {
	ErrorDetails map[string]string
	Config       *ConfigModuleInfo
}

type ConfigModuleInfo struct {
	entity.Config
	Valid bool
}
