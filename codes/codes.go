package codes

const (
	// Common
	StartHttpServerError    = 1001
	ShutdownHttpServerError = 1002
	ShutdownHttpServerInfo  = 1003

	WebsocketError     = 1004
	WebsocketEmitError = 1005

	DatabaseOperationError = 1006

	// Raft
	InitRaftError              = 1007
	RestoreFromRepositoryError = 1008
	RaftLoggerCode             = 1009
	BootstrapClusterError      = 1010
	RaftShutdownError          = 1011
	RaftShutdownInfo           = 1012

	// Raft Log
	ApplyLogCommandError   = 1013
	PrepareLogCommandError = 1014
	SyncApplyError         = 1015

	// Raft Cluster
	SendDeclarationToLeaderError = 1016
	LeaderClientConnectionError  = 1017
	LeaderClientDisconnected     = 1018

	// Services
	DiscoveryServiceSendModulesError  = 1019
	RoutesServiceSendRoutesError      = 1020
	ConfigServiceBroadcastConfigError = 1021

	// Cmux
	InitCmuxError = 1022
)
