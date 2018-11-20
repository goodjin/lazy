lazy
====

yet another log analyzer

[Input]
1. NSQ
2. file
3. kafka
4. mqtt (topic should not use space, just reduce addtion encode/decode handle)

[Output]
1. elasticsearch
2. kafka

[filter]
1. regexp
2. bayes
3. geoip

[Parser]
1. rfc3164
2. customschema (token parser)
3. keyvalue (json format)
4. default rawdata

Todo
Add more input/output, maybe influxdb and so on.
Add LSTM filter
