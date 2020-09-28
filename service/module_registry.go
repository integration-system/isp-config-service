package service

import (
	"sort"

	"github.com/integration-system/isp-lib/v2/structure"
	log "github.com/integration-system/isp-log"
	"isp-config-service/cluster"
	"isp-config-service/codes"
	"isp-config-service/domain"
	"isp-config-service/entity"
	"isp-config-service/holder"
	"isp-config-service/model"
	"isp-config-service/store/state"
)

var (
	ModuleRegistry = moduleRegistryService{}
)

type moduleRegistryService struct{}

func (s moduleRegistryService) HandleModuleConnectedCommand(module entity.Module, state state.WritableState) {
	existedModule := state.WritableModules().GetByName(module.Name)
	if existedModule == nil {
		state.WritableModules().Create(module)
	} else {
		module.Id = existedModule.Id
		module.CreatedAt = existedModule.CreatedAt
		module.LastDisconnectedAt = existedModule.LastDisconnectedAt
		state.WritableModules().UpdateByName(module)
	}
	if holder.ClusterClient.IsLeader() {
		// TODO handle db errors
		_, err := model.ModuleRep.Upsert(module)
		if err != nil {
			log.WithMetadata(map[string]interface{}{
				"module": module,
			}).Errorf(codes.DatabaseOperationError, "upsert module: %v", err)
		}
	}
}

func (moduleRegistryService) HandleModuleDisconnectedCommand(module entity.Module, state state.WritableState) {
	existedModule := state.WritableModules().GetByName(module.Name)
	if existedModule == nil {
		state.WritableModules().Create(module)
	} else {
		module.Id = existedModule.Id
		module.CreatedAt = existedModule.CreatedAt
		module.LastConnectedAt = existedModule.LastConnectedAt
		state.WritableModules().UpdateByName(module)
	}
	if holder.ClusterClient.IsLeader() {
		// TODO handle db errors
		_, err := model.ModuleRep.Upsert(module)
		if err != nil {
			log.WithMetadata(map[string]interface{}{
				"module": module,
			}).Errorf(codes.DatabaseOperationError, "upsert module: %v", err)
		}
	}
}

func (moduleRegistryService) HandleDeleteModulesCommand(deleteModules cluster.DeleteModules, state state.WritableState) int {
	ids := deleteModules.Ids
	deletedModules := state.WritableModules().DeleteByIds(ids)

	configsToDelete := state.WritableConfigs().GetByModuleIds(ids)
	confIds := make([]string, 0, len(configsToDelete))
	for i := range configsToDelete {
		confIds = append(confIds, configsToDelete[i].Id)
	}
	state.WritableConfigs().DeleteByIds(confIds)

	schemasToDelete := state.WritableSchemas().GetByModuleIds(ids)
	schemaIds := make([]string, 0, len(schemasToDelete))
	for _, schema := range schemasToDelete {
		schemaIds = append(schemaIds, schema.Id)
	}
	state.WritableSchemas().DeleteByIds(schemaIds)

	if holder.ClusterClient.IsLeader() {
		// TODO handle db errors
		_, err := model.ModuleRep.Delete(ids)
		if err != nil {
			log.WithMetadata(map[string]interface{}{
				"moduleIds": ids,
			}).Errorf(codes.DatabaseOperationError, "delete modules: %v", err)
		}
		_, err = model.ConfigRep.Delete(confIds)
		if err != nil {
			log.WithMetadata(map[string]interface{}{
				"configIds": confIds,
			}).Errorf(codes.DatabaseOperationError, "delete configs: %v", err)
		}
		_, err = model.SchemaRep.Delete(schemaIds)
		if err != nil {
			log.WithMetadata(map[string]interface{}{
				"schemaIds": schemaIds,
			}).Errorf(codes.DatabaseOperationError, "delete schemas: %v", err)
		}
	}
	return len(deletedModules)
}

func (moduleRegistryService) ValidateConfig(config entity.Config, state state.ReadonlyState) bool {
	schemas := state.Schemas().GetByModuleIds([]string{config.ModuleId})
	valid := false

	for _, s := range schemas {
		dataForValidate := ConfigService.CompileConfig(config.Data, state, config.CommonConfigs...)
		valid, _ = ConfigService.validateSchema(s, dataForValidate)
	}

	return valid
}

//nolint:funlen
func (moduleRegistryService) GetAggregatedModuleInfo(state state.ReadonlyState) []domain.ModuleInfo {
	modules := state.Modules().GetAll()
	modulesLen := len(modules)
	result := make([]domain.ModuleInfo, 0, modulesLen)
	resMap := make(map[string]domain.ModuleInfo, modulesLen)
	nameIdMap := make(map[string]string)
	idList := make([]string, modulesLen)

	for i, module := range modules {
		idList[i] = module.Id
		active := state.Mesh().BackendExist(structure.BackendDeclaration{ModuleName: module.Name})
		info := domain.ModuleInfo{
			Id:                 module.Id,
			Name:               module.Name,
			Active:             active,
			CreatedAt:          module.CreatedAt,
			LastConnectedAt:    module.LastConnectedAt,
			LastDisconnectedAt: module.LastDisconnectedAt,
		}
		resMap[module.Id] = info
		nameIdMap[module.Name] = module.Id
	}

	configs := state.Configs().GetByModuleIds(idList)
	for i := range configs {
		info := resMap[configs[i].ModuleId]
		info.Configs = append(info.Configs, domain.ConfigModuleInfo{
			Config: configs[i],
			Valid:  false,
		})
		resMap[configs[i].ModuleId] = info
	}

	schemas := state.Schemas().GetByModuleIds(idList)
	for _, s := range schemas {
		info := resMap[s.ModuleId]
		schema := s.Schema
		info.ConfigSchema = &schema

		for i := range info.Configs {
			dataForValidate := ConfigService.CompileConfig(info.Configs[i].Data, state, info.Configs[i].CommonConfigs...)
			info.Configs[i].Valid, _ = ConfigService.validateSchema(s, dataForValidate)
		}

		resMap[s.ModuleId] = info
	}

	for key := range resMap {
		info := resMap[key]
		backends := state.Mesh().GetBackends(info.Name)
		conns := make([]domain.Connection, 0, len(backends))

		for _, back := range backends {
			if info.RequiredModules == nil { //get module dependencies from first connected backend
				requiredModules := make([]domain.ModuleDependency, 0, len(back.RequiredModules))
				for _, dep := range back.RequiredModules {
					requiredModules = append(requiredModules, domain.ModuleDependency{
						Name:     dep.Name,
						Id:       nameIdMap[dep.Name],
						Required: dep.Required,
					})
				}
				info.RequiredModules = requiredModules
			}

			con := domain.Connection{
				Version:    back.Version,
				LibVersion: back.LibVersion,
				Address:    back.Address,
				Endpoints:  back.Endpoints,
			}
			sort.Slice(con.Endpoints, func(i, j int) bool {
				return con.Endpoints[i].Path < con.Endpoints[j].Path
			})
			con.EstablishedAt = info.LastConnectedAt
			conns = append(conns, con)
		}
		sort.Slice(info.Configs, func(i, j int) bool {
			return info.Configs[i].Version < info.Configs[j].Version
		})
		sort.Slice(conns, func(i, j int) bool {
			return conns[i].EstablishedAt.Before(conns[j].EstablishedAt)
		})
		info.Status = conns
		result = append(result, info)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}
