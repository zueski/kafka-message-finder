# Kafka Message finder

This program allows you to easily look at a single record header and payload values at a particular partition and offset.

## Usage
There are two major forms for use:
### Explicit form
`java -jar kafkamessagefinder-0.0.1-SNAPSHOT.jar --config=connection.properties --topic=abc --partition=0 --offset=0 --count=1`
where
`--config` is the properties file to create the Kafka client with
`--topic` is the topic name for the message(s) to read from
`--partition` is the partition number to read from
`--offset` is the offset to read from
`--count` is optional and assumed to be 1
### Implict form
`java -jar kafkamessagefinder-0.0.1-SNAPSHOT.jar --correlation=connection/topic/0/0 --count=1`
where
`--correlation` is a delimited string with the following fields:
1. config file name, looks in a folder called `config` in the current working directory and appends `.properties`, i.e., `config/${config name}.properties`
2. topic name to read from
3. partition number to read from 
4. offset to read from

`--count` is optional and assumed to be 1

Sample outout:
```
λ java -jar kafkamessagefinder-0.0.1-SNAPSHOT.jar --correlation=ABC/mytopic/2/4139
======================
 Kafka Message Finder
======================
Starting KafkamessagefinderApplication v0.0.1-SNAPSHOT on xxxx with PID 28372 (C:\Users\aiugh17\admin\kakfaMessageFinder\kafkamessagefinder-0.0.1-SNAPSHOT.jar
No active profile set, falling back to default profiles: default
Started KafkamessagefinderApplication in 1.982 seconds (JVM running for 2.74)
Application started with command-line arguments: [--correlation=ABC/mytopic/2/4139]
--count assumed as 1
attempting to parse log correlation ID: ABC/mytopic/2/4139
Connecting to broker
Successfully logged in.
Kafka version: 2.4.0
Kafka commitId: 77a89fcf8d7fa018
Kafka startTimeMs: 1582902523344
listing partitions
[Consumer clientId=consumer-message-finder-1, groupId=message-finder] Cluster ID: lkc-l6vrj
Found partition 0
Found partition 5
Found partition 4
Found partition 1
Found partition 2
found 87 records
***** Found headers record for topic mytopic, partition 2, offset 4139 with key 'key123' at 2020-02-28T10:01:01.962-05:00[America/New_York] *****
'x-source-event-id' -> '123'
'x-source-event-date' -> '2020-02-28T10:01:01.961758-05:00'
===== Found data record for topic mytopic, partition 2, offset 4139 with key 'key123' ========
"VGhlcmUgaXMgbm8gZGF0YSBpbiB0aGUgbWVzc2FnZSwgYWxsIGluZm8gZm91bmQgaW4gaGVhZGVycw=="

```