package controller

import (
	"github.com/integration-system/isp-lib/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"isp-config-service/cluster"
	"isp-config-service/domain"
	"isp-config-service/entity"
	"isp-config-service/store"
	"isp-config-service/store/state"
)

var Config *config

type config struct {
	rstore *store.Store
}

// GetActiveConfigByModuleName godoc
// @Summary Метод получения объекта конфигурации по названию модуля
// @Description Возвращает активную конфиграцию по названию модуля
// @Tags Конфигурация
// @Accept  json
// @Produce  json
// @Param body body domain.GetByModuleIdRequest true "название модуля"
// @Success 200 {object} entity.Config
// @Failure 404 {object} structure.GrpcError "если конфигурация не найдена"
// @Failure 500 {object} structure.GrpcError
// @Router /config/get_active_config_by_module_name [POST]
func (c *config) GetActiveConfigByModuleName(request domain.GetByModuleIdRequest) (*entity.Config, error) {
	var response *entity.Config
	c.rstore.VisitReadonlyState(func(state state.ReadonlyState) {
		response = state.Configs().GetActiveByModuleId(request.ModuleId)
	})
	if response == nil {
		return nil, status.Error(codes.NotFound, utils.ValidationError)
	}
	return response, nil
}

// CreateUpdateConfig godoc
// @Summary Метод обновления конфигурации
// @Description Если конфиг с таким id существует, то обновляет данные, если нет, то добавляет данные в базу
// В случае обновления рассылает все подключенным модулям актуальную конфигурацию
// @Tags Конфигурация
// @Accept  json
// @Produce  json
// @Param body body entity.Config true "объект для сохранения"
// @Success 200 {object} entity.Config
// @Failure 404 {object} structure.GrpcError "если конфигурация не найдена"
// @Failure 500 {object} structure.GrpcError
// @Router /config/create_update_config [POST]
func (c *config) CreateUpdateConfig(config entity.Config) (*entity.Config, error) {
	var response entity.Config
	now := state.GenerateDate()
	config.CreatedAt = now
	config.UpdatedAt = now
	upsertConfig := cluster.UpsertConfig{
		Config: config,
	}
	if config.Id == "" {
		upsertConfig.Config.Id = state.GenerateId()
		upsertConfig.Create = true
	}
	command := cluster.PrepareUpsertConfigCommand(upsertConfig)
	err := PerformSyncApplyWithError(command, "UpsertConfigCommand", &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// MarkConfigAsActive godoc
// @Summary Метод активации конфигурации для модуля
// @Description Активирует указанную конфигурацию и деактивирует остальные, возвращает активированную конфигурацию
// @Tags Конфигурация
// @Accept  json
// @Produce  json
// @Param body body domain.ConfigIdRequest true "id конфигурации для изменения"
// @Success 200 {object} entity.Config "активированная конфигурация"
// @Failure 404 {object} structure.GrpcError "если конфигурация не найдена"
// @Failure 500 {object} structure.GrpcError
// @Router /config/mark_config_as_active [POST]
func (c *config) MarkConfigAsActive(identity domain.ConfigIdRequest) (*entity.Config, error) {
	var response entity.Config
	command := cluster.PrepareActivateConfigCommand(identity.Id, state.GenerateDate())
	err := PerformSyncApplyWithError(command, "ActivateConfigCommand", &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// DeleteConfigs godoc
// @Summary Метод удаления объектов конфигурации по идентификаторам
// @Description Возвращает количество удаленных модулей
// @Tags Конфигурация
// @Accept  json
// @Produce  json
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
	err := PerformSyncApply(command, "DeleteConfigsCommand", &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func NewConfig(rstore *store.Store) *config {
	return &config{
		rstore: rstore,
	}
}