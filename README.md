# 统一说明
- 此包适用于阿里云实时计算  Blink SQL 中使用  
- mvn clean package 打包后会出现 dsky-blink-1.0-SNAPSHOT.jar 和 dsky-blink-dependency.jar 两个包  
分别是业务包和依赖包  
- KafkaSink和HdfsSink 只需要引入业务包资源即可
- HBaseSetIfNotExistUdf 需要两个包同时引入

# 一、KafkaSink 说明  

### 功能  
>blink的kafka-sink只支持将消息发到单个topic，不支持按照输入字段分发到不同的topic，
而业务需要这样的sink。自定义KafkaSink的功能就是根据输入字段topic指定message应该发到哪个topic,
同时支持将事件时间(ts)作为kafka的timestamp

### 使用示例  

```
create table kafka_sink_out (
    `message` varchar, 
    `topic` varchar, 
    `ts` BIGINT)
with (
    type = 'custom',
    class = 'com.idreamsky.dc.blink.sink.KafkaSink'
    bootstrap.servers = '192.168.118.110'
);
```

### 字段定义  

| 字段名 | 类型   | 必填|说明 |
|----- | ---------|--- | ---|
| message | varchar| 是 | 消息 |
| topic | varchar | 是 |消息转发到哪个topic |
| ts | BIGINT |  否   | 事件时间，秒，不填则使用系统时间作为kafka消息的timestamp |

### 配置项(with) 

#### 默认配置  

| 参数名 | 参数值   | 说明 |
| ----- | --------- | --- |
| bootstrap.servers | 172.16.1.65:9092,172.16.1.66:9092,172.16.1.64:9092 | |
| acks  | all     | |
| retries  | 10     | 消息发送失败重试10次 |
| key.serializer  | org.apache.kafka.common.serialization.StringSerializer     ||
| value.serializer  | org.apache.kafka.common.serialization.StringSerializer     ||

#### 其他 kafka 可用配置  

详见官网 [官方 Producer 配置项说明](https://kafka.apache.org/0102/documentation.html?spm=a2c4g.11186623.2.15.3ff07869qO7K2h#producerconfigs)
     

# 二、HdfsSink 说明  

### 功能 
>将数据落地到 `hdfs` 并支持通过`hive.path.schema`自定义数据分区规则  
支持按时间间隔将数据刷盘，通过`flush.interval`设置，单位是秒  
HdfsSink会自动关闭超过20分钟没有数据写入的文件(后续需要写还会打开)


### 使用示例  
```
create table hdfs_test_out (
    `message` varchar,
    `table` varchar,
    `date` varchar,
    `hour` varchar
)with(
    type = 'custom',
    class = 'com.idreamsky.dc.blink.sink.HdfsSink',
    hive.path.schema = '/user/hive/warehouse/test.db/colby.luo/{table}/log_date={date}/log_hour={hour}'
);
```  

### 字段定义 
| 字段名 | 类型   | 必填|说明 |
| ----- | ---------|--- | ---|
| message | varchar| 是 | 消息 |
| name | varchar | 否 | name字段的值会替换 `hive.path.schema` 中的 {name} |

### 配置项 


| 参数名 | 默认值  | 说明 |
| ----- | --------- | --- |
| hdfs.namenode | 172.16.18.10:8020,172.16.18.9:8020 |  集群节点，现在写死了必须为逗号分隔的两个地址|
| hive.path.schema | /user/hive/warehouse/test.db/{table}/log_date={date}/log_hour={hour} | 数据写入路径，目录级别(不能指定到文件级别)  路径中`{占位符}`可以被数据字段替换，从而实现动态分区的功能|
| flush.interval | 30 |  数据刷盘间隔，单位秒|  
>ps: blink自带有间隔3分钟的同步定时任务

# 三、HBaseSetIfNotExistUdf 说明 
### 功能 
>实现 Set If Not Exist 的原子性操作语义，不存在并设值则返回 1 ，已存在则返回 0

### 使用示例  
```
CREATE FUNCTION new_user_udf AS 'com.idreamsky.dc.blink.udx.HBaseSetIfNotExistUdf';

CREATE TABLE print_sink(
  num INT
)with(
  type = 'print'
);

insert into print_sink
select 
    sum(new_user_udf(rowKey, CAST (ts as varchar))) as num
FROM row_view
```  

### 作业参数

| 参数名 | 默认值  | 说明 |
| ----- | --------- | --- |
| hbase.table.name | new_user| hbase的默认表名，不填则调用函数时必填 |
| hbase.column.name | time |  hbase的默认列名（列族固定为`cf`不可配置和指定） |
| hbase.zookeeper.quorum | 已设置为线上默认值 |  hbase的zk地址|
| hbase.client.username | 已设置为线上默认值 |  |
| hbase.client.password | 已设置为线上默认值 |  |

### 函数调用方法

- Long eval(String rowKey, Object value)  
- Long eval(String rowKey, Object value, String tableName)  
- Long eval(String rowKey, Object value, String tableName, String columnName)  

| 参数名 |  说明 |
| ----- |  --- |
| rowKey |  hbase的rowKey |
| value |  需要设置的值 |
| tableName |  hbase 的表名，配置了默认表名时可以不填 |
| columnName | 列名，配置了默认列名时可以不填 |
>ps: 总之这四个值都得有，不是在作业参数配置就是在调用时指定

