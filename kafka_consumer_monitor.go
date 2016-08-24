package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"net/smtp"
	"os"
	"strconv"
	"strings"
	"time"
	"io/ioutil"
)

var host = ""
var hostAndPort = ""
var username = ""
var password = ""
var receiver = ""

var zkConnectStr = []string{"10.255.209.47:2181"}
var zkNamespace = "/kafka"
var topicName = "orderEventTopic"
var consumerGroupName = "spoutId_AllDimensionTopology"


func main() {
	//host = "smtp.yeah.net"
	//hostAndPort = "smtp.yeah.net:25"
	//username = "wangsc@yeah.net"
	//password = "xxx"
	//receiver = "chunsw@126.com,wsc.hi@163.com"

	//zkConnectStr := []string{"10.255.209.47:2181"}
	//zkNamespace := "/kafka"
	//topicName := "orderEventTopic"
	//consumerGroupName := "test-consumer-group"
	//consumerGroupName := "spoutId_AllDimensionTopology"
	var maxPendingMessage = 1
	var checkIntervalSeconds = 1
	// get config from file
	{
		b, _ := ioutil.ReadFile("config.conf")
		if b == nil || len(b) < 1 {
			fmt.Printf("file not exists: config.conf\n")
			os.Exit(1)
		}
		str := string(b)
		println("content of file 'config.conf':")
		println(str)
		m := make(map[string]string)
		var arr []string
		if strings.Contains(str, "\r\n") {
			arr = strings.Split(str, "\r\n")
		} else {
			arr = strings.Split(str, "\n")
		}
		for _, v := range arr {
			v = strings.Trim(v, " \r\n")
			if len(v) > 1 && []int32(v)[0] != '#' && strings.Contains(v, "=") {
				//println(v)
				pairs := strings.Split(v, "=")
				m[strings.Trim(pairs[0], " ")] = strings.Trim(pairs[1], " ")
			}
		}
		fmt.Printf("%s", m)

		host = m["host"]
		hostAndPort = m["hostAndPort"]
		username = m["username"]
		password = m["password"]
		receiver = m["receiver"]
		zkConnectStr = strings.Split(m["zkConnectStr"], ",")
		zkNamespace = m["zkNamespace"]
		topicName = m["topicName"]
		consumerGroupName = m["consumerGroupName"]
		maxPendingMessage, _ = strconv.Atoi(m["maxPendingMessage"])
		checkIntervalSeconds, _ = strconv.Atoi(m["checkIntervalSeconds"])
	}

	

	// connect to zookeeper
	zkConn, _, err := zk.Connect(zkConnectStr, 3*time.Second)
	if err != nil {
		panic(err)
	}
	zkConn.SetLogger(log.New(os.Stdout, "[zk] ", log.LstdFlags))

	// get kafka broker server address
	servers := getKafkaServers(zkNamespace, zkConn)
	if servers == nil || len(servers) < 1 {
		fmt.Printf("can't find kafka brokers from zookeeper %s\n", zkConnectStr)
		os.Exit(1)
	}

	// connect to kafka
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	client, _err := sarama.NewClient(servers, nil)
	if _err != nil {
		fmt.Printf("can't connect to broker %s. %s", servers, _err)
		os.Exit(1)
	}

	// validate whether the topic exists
	topics, _err := client.Topics()
	fmt.Printf("topics: %v\n", topics)
	if !inArray(topics, topicName) {
		fmt.Printf("topic %s not exists: %s", topicName, topics)
		os.Exit(1)
	}

	// timer for check message pending
	timer := time.NewTicker(time.Duration(checkIntervalSeconds) * time.Second)
	for {
		select {
		case <-timer.C:
			go func() {
				checkPendingMessage(zkNamespace, zkConn, client, topicName, consumerGroupName, maxPendingMessage)
			}()
		}
	}

	/* partitions, _err := client.Partitions(topics[0])
	fmt.Printf("partitions: %v\n", partitions)

	offsetNew, _err := client.GetOffset("orderEventTopic", 0, sarama.OffsetNewest)
	offsetOld, _err := client.GetOffset("orderEventTopic", 0, sarama.OffsetOldest)
	fmt.Printf("offset is: %v - %v", offsetOld, offsetNew)

	// log.Fatalln("xxx: ", client.Config())
	fmt.Printf("config: ", client.Config())
	client.Close()
	*/
}

// check and output the pending message summary
func checkPendingMessage(zkNamespace string, zkConn *zk.Conn, client sarama.Client, topicName, consumerGroupName string, maxPendingMessage int) {
	partitions, _ := client.Partitions(topicName)
	foundFlag := false
	// check message of kafka native consumer
	for _, p := range partitions {
		path := strings.Join([]string{zkNamespace, "/consumers/", consumerGroupName, "/offsets/", topicName, "/", strconv.FormatInt(int64(p), 10)}, "")
		data, _, _ := zkConn.Get(path)
		if data != nil && len(data) > 0 {
			foundFlag = true
			offsetNew, _ := client.GetOffset(topicName, p, sarama.OffsetNewest)
			offset, _ := strconv.ParseInt(string(data), 10, 64)
			if offset+int64(maxPendingMessage) <= offsetNew {
				fmt.Printf("pending message count is %v, threshold is %v: topic %s, consumer %s, partition %v\n", offsetNew-offset, maxPendingMessage, topicName, consumerGroupName, p)
				sendMail(topicName, consumerGroupName, fmt.Sprintf("pending message count is %v, threshold is %v: topic %s, consumer %s, partition %v\n", offsetNew-offset, maxPendingMessage, topicName, consumerGroupName, p))
			}
		}
	}

	if foundFlag {
		return
	}

	// check message of storm-kafka consumer
	for _, p := range partitions {
		path := strings.Join([]string{"/transactional/", consumerGroupName, "/user/partition_", strconv.FormatInt(int64(p), 10)}, "")
		children, _, _ := zkConn.Children(path)
		if children == nil || len(children) < 1 {
			continue
		}

		data, _, _ := zkConn.Get(strings.Join([]string{path, "/", children[0]}, ""))
		if data != nil && len(data) > 0 {
			offsetNew, _ := client.GetOffset(topicName, p, sarama.OffsetNewest)
			m := make(map[string]interface{})
			_ = json.Unmarshal(data, &m)
			offset := int64(m["offset"].(float64))
			if offset+int64(maxPendingMessage) <= offsetNew {
				fmt.Printf("[storm-kafka] pending message count is %v, threshold is %v: topic %s, consumer %s, partition %v\n", offsetNew-offset, maxPendingMessage, topicName, consumerGroupName, p)
				sendMail(topicName, consumerGroupName, fmt.Sprintf("[storm-kafka] pending message count is %v, threshold is %v: topic %s, consumer %s, partition %v\n", offsetNew-offset, maxPendingMessage, topicName, consumerGroupName, p))
			}
		}
	}
}

// send mail
func sendMail(topicName, consumerGroupName, body string) {
	// Set up authentication information.
	auth := smtp.PlainAuth("", username, password, host)

	// Connect to the server, authenticate, set the sender and recipient,
	// and send the email all in one step.
	to := strings.Split(receiver, ",")
	msg := []byte("To: " + receiver +
		"\nSubject: kafka message pending monitor - " + topicName + " - " + consumerGroupName +
		"\r\n\r\n" + body + "\r\n\r\nCurrent time: " + fmt.Sprintf("%s", time.Now()) + 
		" \r\n")
	err := smtp.SendMail(hostAndPort, auth, username, to, msg)
	logger := log.New(os.Stdout, "[me] ", log.LstdFlags)
	if err != nil {
		logger.Fatal(err)
	}

}

// check whether the string in the string array
func inArray(topics []string, topic string) bool {
	for _, v := range topics {
		if v == topic {
			return true
		}
	}
	return false
}

// get kafka broker server address from zookeeper
func getKafkaServers(zkNamespace string, zkConn *zk.Conn) []string {
	children, _, _ := zkConn.Children(strings.Join([]string{zkNamespace, "/brokers/ids"}, ""))
	servers := make([]string, len(children))
	for i, v := range children {
		data, _, _ := zkConn.Get(strings.Join([]string{zkNamespace, "/brokers/ids/", v}, ""))
		m := make(map[string]interface{})
		//m := make(map[string]string)
		err := json.Unmarshal(data, &m)
		if err != nil {
			fmt.Println("error:", err)
		}
		//fmt.Printf("%+v   %v %s", m, i, string(data))
		servers[i] = strings.Join([]string{m["host"].(string), strconv.FormatFloat(m["port"].(float64), 'g', -1, 32)}, ":")
	}
	return servers
}
