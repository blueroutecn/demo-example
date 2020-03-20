# Spark-demo
Spark 的示例代码
## Spark core

## Spark sql
1. UdafSqlObject
    - 用户自定义函数
## Spark streaming

1. 文本文件做单词统计
    - FileWordCountStreamingObject.scala
    - textFileStream 文件中获取数据流

2. Socket 连接做单词统计
    - WordCountStreamingObject.scala
    - socketTextStream socket中获取数据流
    
3. 用户自定义(仿照socket 接收器)
    - UserDefineWordCountStreamingObject.scala
    - extends Receiver 接收器
    - receiverStream 指定接收器
    
4. 连接 Kafka 的消费者做单词统计
    - KafkaCountStreamingObject.scala
    - KafkaUtils.createDirectStream
    - kafkaParams kafka 的参数设置
5. 有状态的 streaming 持续统计(记录历史)
    - UpdateStateObject
    - sparkContext.setCheckpointDir 设置检查点目录
    - updateStateByKey 更新状态
6. 窗口操作
    - WindowSlidingObject.scala
    - window(windowDuration,slidingDuration) 两个参数必须是采集周期的整数倍
    - window 之前必须map成可以序列化的DStream(ConsumerRecord 不可序列化)
    
# flume-demo
1. 自定义拦截器(对接kafka sink)
    - TypeInterceptor.java
    - implements Interceptor 接口
    - Interceptor.Builder 工厂
    - 包含hello字符串 发送给first 主题
    - 不包含的hello 发送给 sencond 主题
    
# hbase-demo
1. 判断表存在

# storm-demo
1. 随机字符数据源
    - RandomToplogic.java 处理器
    - RandonmSpout.java     随机字符串数据源
    - WrapStarBolt  星号处理器
    - WrapWellBolt  井号处理器
2. 读取 website 文件打印
    - WebCountGenerator 文件生成器
    - WebCountSpout  水源，读取文件放入 stream
    - WebCountBolt   阀门，输出 session_id 信息
    - WebCountTopology  拓扑逻辑