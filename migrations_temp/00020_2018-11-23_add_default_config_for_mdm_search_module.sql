-- +goose Up
set rabbit.host = '10.250.9.117';
set rabbit.port = '5672';
set rabbit.user = 'guest';
set rabbit.password = 'guest';

set elastic.url = 'http://10.250.9.48:9200';

WITH moduleVar AS (
    INSERT INTO modules (instance_id, name, active) VALUES (1, 'mdm-search', 't') RETURNING id
)

INSERT INTO configs (module_id, active, data) VALUES (
  (SELECT id FROM moduleVar),
  't',
  ((format('{
  "queue": {
    "prefetchSize": 1000,
    "searcherQueue": "searcher.queue",
    "concurrentConsumers": 4
  },
  "rabbit": {
    "user": "%s",
    "address": {
      "ip": "%s",
      "port": "%s"
    },
    "password": "%s"
  },
  "elastic": {
    "uRL": "%s",
    "shards": 1
  },
  "metrics": {
    "address": {
      "ip": "0.0.0.0",
      "path": "/metrics",
      "port": "9573"
    },
    "gc": true,
    "memory": true
  },
  "isFieldsListWhite": false,
  "fieldsList": [
    "documents",
    "citizen_relatives",
    "contacts",
    "addresses",
    "escredentials",
    "users"
  ]
}', current_setting('rabbit.user'),
    current_setting('rabbit.host'),
    current_setting('rabbit.port'),
    current_setting('rabbit.password'),

    current_setting('elastic.url')
)) :: jsonb));

-- +goose Down

