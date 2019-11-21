package cluster

import (
	"bytes"
	"encoding/binary"
	"github.com/integration-system/isp-lib/structure"
	log "github.com/integration-system/isp-log"
	"isp-config-service/codes"
	"isp-config-service/entity"
	"time"
)

const (
	ApplyCommandEvent = "CONFIG_CLUSTER:APPLY_COMMAND"

	TokenParam   = "token"
	ClusterParam = "cluster"
)
const (
	_ = iota
	UpdateBackendDeclarationCommand
	DeleteBackendDeclarationCommand

	UpdateConfigSchemaCommand

	ModuleConnectedCommand
	ModuleDisconnectedCommand
	DeleteModulesCommand

	ActivateConfigCommand
	DeleteConfigsCommand
	UpsertConfigCommand

	DeleteCommonConfigsCommand
	UpsertCommonConfigCommand
)

func PrepareUpdateBackendDeclarationCommand(backend structure.BackendDeclaration) []byte {
	return prepareCommand(UpdateBackendDeclarationCommand, backend)
}

func PrepareDeleteBackendDeclarationCommand(backend structure.BackendDeclaration) []byte {
	return prepareCommand(DeleteBackendDeclarationCommand, backend)
}

func PrepareModuleConnectedCommand(module entity.Module) []byte {
	return prepareCommand(ModuleConnectedCommand, module)
}

func PrepareModuleDisconnectedCommand(module entity.Module) []byte {
	return prepareCommand(ModuleDisconnectedCommand, module)
}

func PrepareDeleteModulesCommand(ids []string) []byte {
	return prepareCommand(DeleteModulesCommand, DeleteModules{Ids: ids})
}

func PrepareActivateConfigCommand(configId string, date time.Time) []byte {
	return prepareCommand(ActivateConfigCommand, ActivateConfig{ConfigId: configId, Date: date})
}

func PrepareDeleteConfigsCommand(ids []string) []byte {
	return prepareCommand(DeleteConfigsCommand, DeleteModules{Ids: ids})
}

func PrepareUpsertConfigCommand(config UpsertConfig) []byte {
	return prepareCommand(UpsertConfigCommand, config)
}

func PrepareUpdateConfigSchemaCommand(schema entity.ConfigSchema) []byte {
	return prepareCommand(UpdateConfigSchemaCommand, schema)
}

func PrepareDeleteCommonConfigsCommand(id string) []byte {
	return prepareCommand(DeleteCommonConfigsCommand, DeleteCommonConfig{Id: id})
}

func PrepareUpsertCommonConfigCommand(config UpsertCommonConfig) []byte {
	return prepareCommand(UpsertCommonConfigCommand, config)
}

func prepareCommand(command uint64, payload interface{}) []byte {
	cmd := make([]byte, 8)
	binary.BigEndian.PutUint64(cmd, command)
	buf := bytes.NewBuffer(cmd)
	err := json.NewEncoder(buf).Encode(payload)
	if err != nil {
		log.Fatalf(codes.PrepareLogCommandError, "prepare log command: %v", err)
	}
	return buf.Bytes()
}
