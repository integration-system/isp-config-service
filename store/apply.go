package store

import (
	"github.com/integration-system/isp-lib/structure"
	"github.com/pkg/errors"
	"isp-config-service/cluster"
	"isp-config-service/domain"
	"isp-config-service/entity"
	"isp-config-service/service"
)

func (s *Store) applyUpdateBackendDeclarationCommand(data []byte) (interface{}, error) {
	declaration := structure.BackendDeclaration{}
	err := json.Unmarshal(data, &declaration)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal structure.BackendDeclaration")
	}
	service.ClusterMeshService.HandleUpdateBackendDeclarationCommand(declaration, s.state)
	return nil, nil
}

func (s *Store) applyDeleteBackendDeclarationCommand(data []byte) (interface{}, error) {
	declaration := structure.BackendDeclaration{}
	err := json.Unmarshal(data, &declaration)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal structure.BackendDeclaration")
	}
	service.ClusterMeshService.HandleDeleteBackendDeclarationCommand(declaration, s.state)
	return nil, nil
}

func (s *Store) applyUpdateConfigSchemaCommand(data []byte) (interface{}, error) {
	schema := entity.ConfigSchema{}
	err := json.Unmarshal(data, &schema)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal entity.ConfigSchema")
	}
	err = service.SchemaService.HandleUpdateConfigSchemaCommand(schema, s.state)
	if err != nil {
		return nil, errors.WithMessage(err, "update config schema")
	}
	return nil, nil
}

func (s *Store) applyModuleConnectedCommand(data []byte) (interface{}, error) {
	module := entity.Module{}
	err := json.Unmarshal(data, &module)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal entity.Module")
	}
	service.ModuleRegistryService.HandleModuleConnectedCommand(module, s.state)
	return nil, nil
}

func (s *Store) applyModuleDisconnectedCommand(data []byte) (interface{}, error) {
	module := entity.Module{}
	err := json.Unmarshal(data, &module)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal entity.Module")
	}
	service.ModuleRegistryService.HandleModuleDisconnectedCommand(module, s.state)
	return nil, nil
}

func (s *Store) applyDeleteModulesCommand(data []byte) (interface{}, error) {
	deleteModules := cluster.DeleteModules{}
	err := json.Unmarshal(data, &deleteModules)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.DeleteModules")
	}
	deleted := service.ModuleRegistryService.HandleDeleteModulesCommand(deleteModules, s.state)
	return domain.DeleteResponse{Deleted: deleted}, nil
}

func (s *Store) applyActivateConfigCommand(data []byte) (interface{}, error) {
	activateConfig := cluster.ActivateConfig{}
	err := json.Unmarshal(data, &activateConfig)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.ActivateConfig")
	}
	response := service.ConfigService.HandleActivateConfigCommand(activateConfig, s.state)
	return response, nil
}

func (s *Store) applyDeleteConfigsCommand(data []byte) (interface{}, error) {
	deleteConfigs := cluster.DeleteConfigs{}
	err := json.Unmarshal(data, &deleteConfigs)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.DeleteConfigs")
	}
	deleted := service.ConfigService.HandleDeleteConfigsCommand(deleteConfigs, s.state)
	return domain.DeleteResponse{Deleted: deleted}, nil
}

func (s *Store) applyUpsertConfigCommand(data []byte) (interface{}, error) {
	config := cluster.UpsertConfig{}
	err := json.Unmarshal(data, &config)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.UpsertConfig")
	}
	response := service.ConfigService.HandleUpsertConfigCommand(config, s.state)
	return response, nil
}

func (s *Store) applyDeleteCommonConfigsCommand(data []byte) (interface{}, error) {
	deleteConfigs := cluster.DeleteCommonConfig{}
	err := json.Unmarshal(data, &deleteConfigs)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.DeleteCommonConfig")
	}
	response := service.CommonConfigService.HandleDeleteConfigsCommand(deleteConfigs, s.state)
	return response, nil
}

func (s *Store) applyUpsertCommonConfigCommand(data []byte) (interface{}, error) {
	config := cluster.UpsertCommonConfig{}
	err := json.Unmarshal(data, &config)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal cluster.UpsertCommonConfig")
	}
	response := service.CommonConfigService.HandleUpsertConfigCommand(config, s.state)
	return response, nil
}
