//nolint:lll
package helper

import (
	"github.com/integration-system/isp-lib/v2/structure"
	"isp-config-service/controller"
	"isp-config-service/domain"
	"isp-config-service/entity"
)

type Handlers struct {
	// ===== MODULE =====
	DeleteModules   func(identities []string) (*domain.DeleteResponse, error)       `method:"delete_module" group:"module" inner:"true"`
	GetModulesInfo  func() ([]domain.ModuleInfo, error)                             `method:"get_modules_info" group:"module" inner:"true"`
	GetModuleByName func(req domain.GetByModuleNameRequest) (*entity.Module, error) `method:"get_by_name" group:"module" inner:"true"`
	BroadcastEvent  func(req domain.BroadcastEventRequest) error                    `method:"broadcast_event" group:"module" inner:"true"`

	// ===== CONFIG =====
	GetActiveConfigByModuleName func(request domain.GetByModuleNameRequest) (*entity.Config, error)             `method:"get_active_config_by_module_name" group:"config" inner:"true"`
	GetConfigsByModuleId        func(request domain.GetByModuleIdRequest) ([]domain.ConfigModuleInfo, error)    `method:"get_configs_by_module_id" group:"config" inner:"true"`
	CreateUpdateConfig          func(config domain.CreateUpdateConfigRequest) (*domain.ConfigModuleInfo, error) `method:"create_update_config" group:"config" inner:"true"`
	MarkConfigAsActive          func(identity domain.ConfigIdRequest) (*entity.Config, error)                   `method:"mark_config_as_active" group:"config" inner:"true"`
	DeleteConfig                func(identities []string) (*domain.DeleteResponse, error)                       `method:"delete_config" group:"config" inner:"true"`
	DeleteVersion               func(identity domain.ConfigIdRequest) (*domain.DeleteResponse, error)           `method:"delete_version" group:"config" inner:"true"`
	GetAllVersion               func(identity domain.ConfigIdRequest) ([]entity.VersionConfig, error)           `method:"get_all_version" group:"config" inner:"true"`
	GetConfigByID               func(identity domain.ConfigIdRequest) (entity.Config, error)                    `method:"get_config_by_id" group:"config" inner:"true"`

	// ===== COMMON CONFIG =====
	GetCommonConfigs         func(identities []string) []entity.CommonConfig                              `method:"get_configs" group:"common_config" inner:"true"`
	CreateUpdateCommonConfig func(config entity.CommonConfig) (*entity.CommonConfig, error)               `method:"create_update_config" group:"common_config" inner:"true"`
	DeleteCommonConfig       func(req domain.ConfigIdRequest) (*domain.DeleteCommonConfigResponse, error) `method:"delete_config" group:"common_config" inner:"true"`
	CompileConfigs           func(req domain.CompileConfigsRequest) domain.CompiledConfigResponse         `method:"compile" group:"common_config" inner:"true"`
	GetLinks                 func(req domain.ConfigIdRequest) domain.CommonConfigLinks                    `method:"get_links" group:"common_config" inner:"true"`

	// ===== ROUTING =====
	GetRoutes func() ([]structure.BackendDeclaration, error) `method:"get_routes" group:"routing" inner:"true"`

	// ===== SCHEMA =====
	GetSchemaByModuleId func(domain.GetByModuleIdRequest) (*entity.ConfigSchema, error) `method:"get_by_module_id" group:"schema" inner:"true"`
}

func GetHandlers() *Handlers {
	return &Handlers{
		DeleteModules:   controller.Module.DeleteModules,
		GetModulesInfo:  controller.Module.GetModulesAggregatedInfo,
		GetModuleByName: controller.Module.GetModuleByName,
		BroadcastEvent:  controller.Module.BroadcastEvent,

		GetActiveConfigByModuleName: controller.Config.GetActiveConfigByModuleName,
		GetConfigsByModuleId:        controller.Config.GetConfigsByModuleId,
		CreateUpdateConfig:          controller.Config.CreateUpdateConfig,
		MarkConfigAsActive:          controller.Config.MarkConfigAsActive,
		DeleteConfig:                controller.Config.DeleteConfigs,
		DeleteVersion:               controller.Config.DeleteConfigVersion,
		GetAllVersion:               controller.Config.GetAllVersion,
		GetConfigByID:               controller.Config.GetConfigById,

		GetCommonConfigs:         controller.CommonConfig.GetConfigs,
		CreateUpdateCommonConfig: controller.CommonConfig.CreateUpdateConfig,
		DeleteCommonConfig:       controller.CommonConfig.DeleteConfigs,
		CompileConfigs:           controller.CommonConfig.CompileConfigs,
		GetLinks:                 controller.CommonConfig.GetLinks,

		GetRoutes: controller.Routes.GetRoutes,

		GetSchemaByModuleId: controller.Schema.GetByModuleId,
	}
}
