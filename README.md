## SparkStreaming流处理分析订单日志

### 一、项目说明
> 订单数据发送到Kafka中，SparkStreaming消费Kafka数据，以天/时/分钟的维度统计下单单数、成交单数、成交金额，将结果写入到Redis中


### 二、模块分解

- 基础模块（配置项类、Redis连接池）
- mock 订单JSON数据发送到Kafka
- 消费Kafka进行统计，使用Zookeeper和Kafka两种方式进行Offset管理
- 储存统计结果到Redis



### 三、项目完整代码
- ⚠️ 需要在/src/main 下创建resources/application.conf

```
HBASE_ROOT_DIR="alluxio://ip:port/hbase"
KAFKA_BROKER_LIST="ip:port"
KAFKA_GROUP_ID="xxx"
KAFKA_TOPIC="xxx"

REDIS_HOST="xxx"
REDIS_DB="1"
ZK_HOST="ip:port"
```

- [完整代码](https://github.com/Onestarko/SteamingAnalysis)

### 四、补充
- [项目细节说明](https://onestarko.github.io/2020/07/09/SparkStreaming流处理分析订单日志/) 

- [SparkStreaming整合Kafka，不同版本的使用差异](https://onestarko.github.io/2020/06/03/SparkStreaming整合Kafka/)
