### v2.4.1
* updated method `/config/get_configs_by_module_id`
### v2.4.0
* add method `/config/get_config_by_id`
* add method `/config/get_configs_by_module_id`
* updated docs
### v2.3.1
* fix old configs order
* add `createdAt` field to config version
### v2.3.0
* add method `/module/broadcast_event` to broadcast arbitrary event to modules
* add module dependencies to response `/module/get_modules_info`
* add option to create a new config version instead update it
* fix broadcasting config on activating and upserting configs
* add saving old configs
* add method `/config/delete_version`
* add method `/config/get_all_version`
* update libs

### v2.2.1
* update deps
* fix bug in json schema marshaling/unmarshaling

### v2.2.0
* migrate to go modules
* add linter, refactor

### v2.1.3
* improved logging

### v2.1.2
* leader ws client ConnectionReadLimit is now configured by WS.WsConnectionReadLimitKB config param and defaults to 4 MB

### v2.1.1
* update to new endpoint descriptor struct with extra fields introduced in isp-lib v2.1.0

### v2.1.0
* fix panic type casting in snapshot
* create empty default config now
* add method `/module/get_by_name` to fetch module object by name

### v2.0.3
* fix data race when applyCommandOnLeader in websocket handler
* update dependencies

### v2.0.2
* fix raft server address announcing (serverId == serverAddress == cluster.outerAddress)

### v2.0.1
* fix nil pointer dereference in repositories
* increase default websocket ConnectionReadLimit to 4 MB, add to configuration

### v2.0.0
* full rewrite to support cluster mode
* change websocket protocol from socketio to etp

### v1.1.0
* update libs
* default config handling
