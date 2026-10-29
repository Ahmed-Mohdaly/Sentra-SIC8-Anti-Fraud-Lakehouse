# Compatibility review v1-draft

Historical CSV (IBM): `User`,`Card`,`Year`,`Month`,`Day`,`Time`,`Amount`,`Use Chip`,`Merchant Name`,`Merchant City`,`Merchant State`,`Zip`,`MCC`,`Errors?`,`Is Fraud?`. `User + Card` only a source-scoped card identity. No native transaction ID, account, precise GPS, true ATM confirmation or terminal outcome. Event time local timezone unknown. Historical labels in isolated table.

Supplied S3 `00000000000000000275.json`: 50 records, has `offset,key,timestamp,value` with **value object** already decoded, event ID, nested `data.transaction_id,user_id,card_id,amount,currency,transaction_type,status,is_fraud`; no `account_id` or precise location. Only positive, approved EUR POS in this file. This is a small integration-test sample, NOT evidence that all events match it. `bronze_adapter` refuses account guessing and separates `is_fraud`.

S3 sink may emit `value` as JSON string in other versions; `unwrap_s3_record` accepts string/object. Kafka offset is only unique *within topic+partition*; source records should add topic, partition, file and run metadata. Missing source timezone and missing location must remain unknown.

Open decisions: canonical account mapping and card namespacing across Kafka and PostgreSQL; Sim EUR vs Historical USD currency policy; exact GPS and terminal failures required by official three scenarios/KPIs; CDC ordering/LSN policy; explicit status vocabulary; schema evolution policy. Do not silently default unknown `account_id` from `user_id`.
