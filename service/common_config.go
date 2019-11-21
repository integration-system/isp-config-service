package service

import (
	log "github.com/integration-system/isp-log"
	codes2 "google.golang.org/grpc/codes"
	"isp-config-service/cluster"
	"isp-config-service/codes"
	"isp-config-service/domain"
	"isp-config-service/holder"
	"isp-config-service/model"
	"isp-config-service/store/state"
)

var (
	CommonConfigService = commonConfigService{}
)

type commonConfigService struct{}

func (s commonConfigService) HandleDeleteConfigsCommand(deleteCommonConfig cluster.DeleteCommonConfig, state state.WritableState) cluster.ResponseWithError {
	links := s.GetCommonConfigLinks(deleteCommonConfig.Id, state)
	if len(links) > 0 {
		return cluster.NewResponse(domain.DeleteCommonConfigResponse{Deleted: false, Links: links})
	}
	ids := []string{deleteCommonConfig.Id}
	deleted := state.WritableCommonConfigs().DeleteByIds(ids)
	if holder.ClusterClient.IsLeader() {
		// TODO handle db errors
		_, err := model.CommonConfigRep.Delete(ids)
		if err != nil {
			log.WithMetadata(map[string]interface{}{
				"configIds": ids,
			}).Errorf(codes.DatabaseOperationError, "delete configs: %v", err)
		}
	}
	return cluster.NewResponse(domain.DeleteCommonConfigResponse{Deleted: deleted > 0})
}

func (s commonConfigService) HandleUpsertConfigCommand(upsertConfig cluster.UpsertCommonConfig, state state.WritableState) cluster.ResponseWithError {
	config := upsertConfig.Config
	configsByName := state.CommonConfigs().GetByName(config.Name)
	if upsertConfig.Create {
		if len(configsByName) > 0 {
			return cluster.NewResponseErrorf(codes2.AlreadyExists, "common config with name %s already exists", upsertConfig.Config.Name)
		}
		config = state.WritableCommonConfigs().Create(config)
	} else {
		// Update
		configs := state.CommonConfigs().GetByIds([]string{config.Id})
		if len(configs) == 0 {
			return cluster.NewResponseErrorf(codes2.NotFound, "common config with id %s not found", config.Id)
		}
		if len(configsByName) > 0 && configs[0].Id != configsByName[0].Id {
			return cluster.NewResponseErrorf(codes2.AlreadyExists, "common config with name %s already exists", upsertConfig.Config.Name)
		}
		config.CreatedAt = configs[0].CreatedAt
		state.WritableCommonConfigs().UpdateById(config)
	}

	if holder.ClusterClient.IsLeader() {
		// TODO handle db errors
		_, err := model.CommonConfigRep.Upsert(config)
		if err != nil {
			log.WithMetadata(map[string]interface{}{
				"config": config,
			}).Errorf(codes.DatabaseOperationError, "upsert common config: %v", err)
		}
	}
	if !upsertConfig.Create {
		configsToBroadcast := state.Configs().FilterByCommonConfigs([]string{config.Id})
		ConfigService.BroadcastNewConfig(state, configsToBroadcast...)
	}
	return cluster.NewResponse(config)
}

func (s commonConfigService) GetCommonConfigLinks(commonConfigId string, state state.ReadonlyState) domain.CommonConfigLinks {
	configs := state.Configs().FilterByCommonConfigs([]string{commonConfigId})
	result := make(domain.CommonConfigLinks)
	for _, c := range configs {
		module := state.Modules().GetById(c.ModuleId)
		if module != nil {
			if configs, ok := result[module.Name]; ok {
				result[module.Name] = append(configs, c.Name)
			} else {
				result[module.Name] = []string{c.Name}
			}
		}
	}
	return result
}
