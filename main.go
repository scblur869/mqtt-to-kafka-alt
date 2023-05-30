/*
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.

 * Contributors:
 *    Matt Brittan primary mqtt contributor "github.com/eclipse/paho.mqtt.golang"
 *    Brad Ross added some parameterization, kafka writer support
 */

package main

// Connect to the broker, subscribe, and write messages received to a file

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	kafka "github.com/segmentio/kafka-go"
)

const (
	WRITETOLOG   = true  // If true then received messages will be written to the console
	WRITETODISK  = false // If true then received messages will be written to the file below
	KAKFACLUSTER = "localhost"
	KAFKA_ON_AWS = true
	USEKAFKA     = false
)

type KAFKA struct {
	URL       string
	Topic     string
	Partition string
}

var (
	kaf KAFKA
)

// handler is a simple struct that provides a function to be called when a message is received. The message is parsed
// and the count followed by the raw message handed off to a kafka writer. You can extend this handler by adding additional service pointers to the struct
/* example
type handler struct {
	kc *kafka.Conn
	kp *kinesis.Producer
	f  *file
}
*/

type handler struct {
	kc *kafka.Conn
}

func init() {
	kaf = KAFKA{
		URL:       os.Getenv("KAFKA_URL"),
		Topic:     os.Getenv("KAFKA_TOPIC"),
		Partition: os.Getenv("KAFKA_PARTITION"),
	}
}

func NewHandler() *handler {
	topic := kaf.Topic
	partition, err := strconv.Atoi(kaf.Partition)
	if err != nil {
		fmt.Println("err converting int")
	}
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	return &handler{kc: conn}
}

// Close closes the file
func (k *handler) Close() {

	if k.kc != nil {
		if err := k.kc.Close(); err != nil {
			fmt.Printf("ERROR closing kafka connection: %s", err)
		}
		k.kc = nil
	}
}

// Message
type Message struct {
	Count uint64
}

// handle is called when a message is received
func (k *handler) handle(_ mqtt.Client, msg mqtt.Message) {
	// We extract the count and write that out first to simplify checking for missing values
	var m Message
	if err := json.Unmarshal(msg.Payload(), &m); err != nil {
		fmt.Printf("Message could not be parsed (%s): %s", msg.Payload(), err)
	}
	k.kc.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := k.kc.WriteMessages(
		kafka.Message{
			Key:   []byte("t-" + strconv.FormatInt(time.Now().UTC().UnixNano(), 10)),
			Value: msg.Payload()},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if WRITETOLOG {
		fmt.Printf("received message: %s\n", msg.Payload())
	}
}

func main() {
	TOPIC := os.Getenv("TOPIC")
	QOS := os.Getenv("QOS")
	BROKER := os.Getenv("BROKER")
	PORT := os.Getenv("PORT")
	USER := os.Getenv("USER")
	PASS := os.Getenv("PASS")
	CLIENTID := os.Getenv("CLIENTID")

	QOSINT, err := strconv.Atoi(QOS)
	if err != nil {
		fmt.Println(err) //
	}

	// Enable logging by uncommenting the below
	// mqtt.ERROR = log.New(os.Stdout, "[ERROR] ", 0)
	// mqtt.CRITICAL = log.New(os.Stdout, "[CRITICAL] ", 0)
	// mqtt.WARN = log.New(os.Stdout, "[WARN]  ", 0)
	// mqtt.DEBUG = log.New(os.Stdout, "[DEBUG] ", 0)

	// Create a handler that will deal with incoming messages
	h := NewHandler()
	defer h.Close()

	// Now we establish the connection to the mqtt broker
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://" + BROKER + ":" + PORT)
	opts.SetClientID(CLIENTID)

	opts.SetOrderMatters(false)       // Allow out of order messages (use this option unless in order delivery is essential)
	opts.ConnectTimeout = time.Second // Minimal delays on connect
	opts.WriteTimeout = time.Second   // Minimal delays on writes
	opts.KeepAlive = 10               // Keepalive every 10 seconds so we quickly detect network outages
	opts.PingTimeout = time.Second    // local broker so response should be quick
	opts.Username = USER
	opts.Password = PASS
	// Automate connection management (will keep trying to connect and will reconnect if network drops)
	opts.ConnectRetry = true
	opts.AutoReconnect = true

	// If using QOS2 and CleanSession = FALSE then it is possible that we will receive messages on topics that we
	// have not subscribed to here (if they were previously subscribed to they are part of the session and survive
	// disconnect/reconnect). Adding a DefaultPublishHandler lets us detect this.
	opts.DefaultPublishHandler = func(_ mqtt.Client, msg mqtt.Message) {
		fmt.Printf("UNEXPECTED MESSAGE: %s\n", msg)
	}

	// Log events
	opts.OnConnectionLost = func(cl mqtt.Client, err error) {
		fmt.Println("connection lost")
	}

	opts.OnConnect = func(c mqtt.Client) {
		fmt.Println("connection established")

		// Establish the subscription - doing this here means that it will happen every time a connection is established
		// (useful if opts.CleanSession is TRUE or the broker does not reliably store session data)
		t := c.Subscribe(TOPIC, byte(QOSINT), h.handle)
		// the connection handler is called in a goroutine so blocking here would hot cause an issue. However as blocking
		// in other handlers does cause problems its best to just assume we should not block
		go func() {
			_ = t.Wait() // Can also use '<-t.Done()' in releases > 1.2.0
			if t.Error() != nil {
				fmt.Printf("ERROR SUBSCRIBING: %s\n", t.Error())
			} else {
				fmt.Println("subscribed to: ", TOPIC)
			}
		}()
	}
	opts.OnReconnecting = func(mqtt.Client, *mqtt.ClientOptions) {
		fmt.Println("attempting to reconnect")
	}

	//
	// Connect to the broker
	//
	client := mqtt.NewClient(opts)

	// If using QOS2 and CleanSession = FALSE then messages may be transmitted to us before the subscribe completes.
	// Adding routes prior to connecting is a way of ensuring that these messages are processed
	client.AddRoute(TOPIC, h.handle)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	fmt.Println("Connection is up")

	// Messages will be delivered asynchronously so we just need to wait for a signal to shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGTERM)

	<-sig
	fmt.Println("signal caught - exiting")
	client.Disconnect(1000)
	fmt.Println("shutdown complete")
}
