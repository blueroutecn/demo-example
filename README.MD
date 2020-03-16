# Spark-demo
Spark 的示例代码
## Spark core

## Spark sql
1. UdafSqlObject
    - 用户自定义函数
## Spark streaming

1. FileWordCountStreamingObject
    - 文本文件做单词统计

2. KafkaCountStreamingObject
    - spark 连接 kafka 的消费者做文本文件做单词统计

3. UserDefineWordCountStreamingObject
    - 用户自定义 仿照 socket 接收器

4. WordCountStreamingObject
    - Socket 连接做单词统计
    
# flume -demo
1. TypeInterceptor
对接kafka的自定义flume拦截器，把 包含 hello字符串 的文本发送给 first 主题
不包含的 发送给 sencond 主题