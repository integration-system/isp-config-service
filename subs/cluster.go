package subs

import (
	etp "github.com/integration-system/isp-etp-go"
	log "github.com/integration-system/isp-log"
	"isp-config-service/cluster"
	"isp-config-service/codes"
	"isp-config-service/holder"
)

func (h *socketEventHandler) applyCommandOnLeader(conn etp.Conn, cmd []byte) []byte {
	obj, err := holder.ClusterClient.SyncApplyOnLeader(cmd)
	if err != nil {
		var logResponse cluster.ApplyLogResponse
		logResponse.ApplyError = err.Error()
		data, err := json.Marshal(obj)
		if err != nil {
			log.Fatalf(codes.SyncApplyError, "marshaling ApplyLogResponse: %v", err)
		}
		return data
	}
	data, err := json.Marshal(obj)
	if err != nil {
		log.Fatalf(codes.SyncApplyError, "marshaling ApplyLogResponse: %v", err)
	}
	return data
}
