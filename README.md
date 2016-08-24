# kafka_consumer_monitor_go
monitor the pending message count of consumer of kafka: native or storm consumer

监控kafka的消费者，在特定topic下的各个partition分区下，有多少未消费的消息；
如果超过特定阈值，打印日志，并发送邮件；

可监控storm-kafka的消费者

执行“go build kafka_consumer_monitor.go”命令生成的可执行文件时，需要在同一目录下放一个config.conf文件，配置要监控的topic等。

配置文件示例：
<pre>
#if your email provider is 126.com, set host to smtp.126.com
host = smtp.yeah.net
hostAndPort = smtp.yeah.net:25
username = xxx@yeah.net
password = xxxx
#if you have multiple receiver, seperate it by comma
receiver = wangshichun@dangdang.com

# "zookeeper.connect" in kafka server.properties
zkConnectStr = 10.255.209.46:2181,10.255.209.47:2181
# if the namespace is the zookeeper root, leave this property blank
zkNamespace = /kafka

# topic and consumer name
topicName = orderEventTopic
consumerGroupName = spoutId_AllDimensionTopology

# the threshod of max pending message count which not processed by the consumer
maxPendingMessage=5
# the time interval of the timer
checkIntervalSeconds=1

</pre>
