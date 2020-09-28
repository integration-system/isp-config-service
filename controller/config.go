package controller

import (
	"time"

	"github.com/integration-system/isp-lib/v2/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"isp-config-service/cluster"
	"isp-config-service/domain"
	"isp-config-service/entity"
	"isp-config-service/service"
	"isp-config-service/store"
	"isp-config-service/store/state"
)

var Config *config

type config struct {
	rstore *store.Store
}

// @Summary Метод получения объекта конфигурации по названию модуля
// @Description Возвращает активную конфиграцию по названию модуля
// @Tags Конфигурация
// @Accept json
// @Produce json
// @Param body body domain.GetByModuleNameRequest true "название модуля"
// @Success 200 {object} entity.Config
// @Failure 404 {object} structure.GrpcError "если конфигурация не найдена"
// @Failure 500 {object} structure.GrpcError
// @Router /config/get_active_config_by_module_name [POST]
func (c *config) GetActiveConfigByModuleName(request domain.GetByModuleNameRequest) (*entity.Config, error) {
	var (
		module *entity.Module
		config *entity.Config
	)
	c.rstore.VisitReadonlyState(func(state state.ReadonlyState) {
		module = state.Modules().GetByName(request.ModuleName)
		if module != nil {
			config = state.Configs().GetActiveByModuleId(module.Id)
		}
	})
	if module == nil {
		return nil, status.Errorf(codes.NotFound, "module with name '%s' not found", request.ModuleName)
	}
	if config == nil {
		return nil, status.Errorf(codes.NotFound, "active config for module '%s' not found", request.ModuleName)
	}
	return config, nil
}

// @Summary Метод получения списка конфигураций по ID модуля
// @Description Возвращает список конфиграции по ID модуля
// @Tags Конфигурация
// @Accept json
// @Produce json
// @Param body body domain.GetByModuleIdRequest true "ID модуля"
// @Success 200 {array}  domain.ConfigModuleInfo
// @Failure 400 {object} structure.GrpcError "если идентификатор не указан"
// @Failure 404 {object} structure.GrpcError "если конфигурация не найдена"
// @Failure 500 {object} structure.GrpcError
// @Router /config/get_configs_by_module_id [POST]
func (c *config) GetConfigsByModuleId(request domain.GetByModuleIdRequest) ([]domain.ConfigModuleInfo, error) {
	var config []entity.Config
	var configInfo []domain.ConfigModuleInfo

	c.rstore.VisitReadonlyState(func(state state.ReadonlyState) {
		config = state.Configs().GetByModuleIds([]string{request.ModuleId})
		if len(config) == 0 {
			return
		}
		for _, value := range config {
			configInfo = append(configInfo, domain.ConfigModuleInfo{
				Config: value,
				Valid:  service.ModuleRegistry.ValidateConfig(value, state),
			})
		}
	})

	if len(configInfo) == 0 {
		return nil, status.Errorf(codes.NotFound, "configs for module '%s' not found", request.ModuleId)
	}

	return configInfo, nil
}

// @Summary Метод обновления конфигурации
// @Description Если конфиг с таким id существует, то обновляет данные, если нет, то добавляет данные в базу
// @Description В случае обновления рассылает всем подключенным модулям актуальную конфигурацию
// @Tags Конфигурация
// @Accept json
// @Produce json
// @Param body body domain.CreateUpdateConfigRequest true "объект для сохранения"
// @Success 200 {object} domain.ConfigModuleInfo
// @Failure 404 {object} structure.GrpcError "если конфигурация не найдена"
// @Failure 500 {object} structure.GrpcError
// @Router /config/create_update_config [POST]
func (c *config) CreateUpdateConfig(config domain.CreateUpdateConfigRequest) (*domain.ConfigModuleInfo, error) {
	var response domain.CreateUpdateConfigResponse
	now := state.GenerateDate()
	config.CreatedAt = now
	config.UpdatedAt = now
	upsertConfig := cluster.UpsertConfig{
		Config:          config.Config,
		Unsafe:          config.Unsafe,
		VersionId:       state.GenerateId(),
		VersionCreateAt: time.Now(),
	}
	if config.Id == "" {
		upsertConfig.Config.Id = state.GenerateId()
		upsertConfig.Create = true
	}
	command := cluster.PrepareUpsertConfigCommand(upsertConfig)
	err := PerformSyncApplyWithError(command, &response)
	if err != nil {
		return nil, err
	} else if response.ErrorDetails != nil {
		return nil, utils.CreateValidationErrorDetails(codes.InvalidArgument, utils.ValidationError, response.ErrorDetails)
	}

	return response.Config, nil
}

// @Summary Метод активации конфигурации для модуля
// @Description Активирует указанную конфигурацию и деактивирует остальные, возвращает активированную конфигурацию
// @Tags Конфигурация
// @Accept json
// @Produce json
// @Param body body domain.ConfigIdRequest true "id конфигурации для изменения"
// @Success 200 {object} entity.Config "активированная конфигурация"
// @Failure 404 {object} structure.GrpcError "если конфигурация не найдена"
// @Failure 500 {object} structure.GrpcError
// @Router /config/mark_config_as_active [POST]
func (c *config) MarkConfigAsActive(identity domain.ConfigIdRequest) (*entity.Config, error) {
	var response entity.Config
	command := cluster.PrepareActivateConfigCommand(identity.Id, state.GenerateDate())
	err := PerformSyncApplyWithError(command, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// @Summary Метод удаления объектов конфигурации по идентификаторам
// @Description Возвращает количество удаленных модулей
// @Tags Конфигурация
// @Accept json
// @Produce json
// @Param body body []string true "массив идентификаторов конфигураций"
// @Success 200 {object} domain.DeleteResponse
// @Failure 400 {object} structure.GrpcError "если не указан массив идентификаторов"
// @Failure 500 {object} structure.GrpcError
// @Router /config/delete_config [POST]
func (c *config) DeleteConfigs(identities []string) (*domain.DeleteResponse, error) {
	if len(identities) == 0 {
		validationErrors := map[string]string{
			"ids": "Required",
		}
		return nil, utils.CreateValidationErrorDetails(codes.InvalidArgument, utils.ValidationError, validationErrors)
	}
	var response domain.DeleteResponse
	command := cluster.PrepareDeleteConfigsCommand(identities)
	err := PerformSyncApply(command, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// @Summary Метод удаления версии конфигурации
// @Description Возвращает количество удаленных версий
// @Tags Конфигурация
// @Accept json
// @Produce json
// @Param body body domain.ConfigIdRequest true "id версии конфигурации"
// @Success 200 {object} domain.DeleteResponse
// @Failure 400 {object} structure.GrpcError "если не указан массив идентификаторов"
// @Failure 500 {object} structure.GrpcError
// @Router /config/delete_version [POST]
func (c *config) DeleteConfigVersion(req domain.ConfigIdRequest) (*domain.DeleteResponse, error) {
	var response domain.DeleteResponse
	command := cluster.PrepareDeleteConfigVersionCommand(req.Id)
	err := PerformSyncApplyWithError(command, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// @Summary Метод получение старых версий конфигурации
// @Description Возвращает предыдущие версии конфигураций
// @Tags Конфигурация
// @Accept json
// @Produce json
// @Param body body domain.ConfigIdRequest true "id конфигурации"
// @Success 200 {array} entity.VersionConfig
// @Failure 400 {object} structure.GrpcError "если не указан массив идентификаторов"
// @Failure 500 {object} structure.GrpcError
// @Router /config/get_all_version [POST]
func (c *config) GetAllVersion(req domain.ConfigIdRequest) ([]entity.VersionConfig, error) {
	var response []entity.VersionConfig
	c.rstore.VisitReadonlyState(func(state state.ReadonlyState) {
		response = state.VersionConfig().GetByConfigId(req.Id)
	})
	return response, nil
}

// @Summary Метод получение актуальной конфигурации конфигурации
// @Description Возвращает актуальную версию конфигурации без дополнительного содержимого (ConfigData)
// @Tags Конфигурация
// @Accept json
// @Produce json
// @Param body body domain.ConfigIdRequest true "id конфигурации"
// @Success 200 {object} entity.Config
// @Failure 400 {object} structure.GrpcError "если не указан идентификатор конфигурации"
// @Failure 500 {object} structure.GrpcError
// @Router /config/get_config_by_id [POST]
func (c *config) GetConfigById(req domain.ConfigIdRequest) (entity.Config, error) {
	var response []entity.Config
	c.rstore.VisitReadonlyState(func(state state.ReadonlyState) {
		response = state.Configs().GetByIds([]string{req.Id})
	})

	if len(response) > 0 {
		response[0].Data = nil
		return response[0], nil
	}

	return entity.Config{}, status.Errorf(codes.NotFound, "config with id '%s' not found", req.Id)
}

func NewConfig(rstore *store.Store) *config {
	return &config{
		rstore: rstore,
	}
}
