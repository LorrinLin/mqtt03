package main

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"log"
	"strconv"
	"sync"
	"math/rand"
	"io/ioutil"
	"time"
	tls "crypto/tls"
	x509 "crypto/x509"
)

var (
	start time.Time
	t int
)

//This application is combined a mqtt consumer and a publisher,
//The publisher publish so many messages(100,1000) to the consumer, and then calculate the using time
func main() {

	var wg, wg2, wg3 sync.WaitGroup
	//public broker, iot.eclipse.org
	//personal broker, 142.93.161.16
	//loacl host, localhost
	uri := "ssl://localhost:8883"
	topic := "testTimeTopic"
	t = 10
		wg.Add(1)
		rand.Seed(time.Now().Unix())
		
		randomId := rand.Float64();
		go listen(uri, topic, randomId, &wg, &wg2)
		wg.Wait()
		publisher := connect("pub", uri)
		
		
		start = time.Now()
		//set a timestamp when it ready to publish

		for i := 0; i < t; i++ {
			payload := "msg:"
			payload += strconv.Itoa(i)
			wg2.Add(1)
			wg3.Add(1)
			go publushMessage(publisher, topic, payload, &wg3)
		}
		wg3.Wait()
		wg2.Wait()
		//after the consumer received the messages, stop time
		duration := time.Since(start)
		log.Println("----how many messages:",t)
		log.Println("---- messages duration: ",duration)

}

func publushMessage(publisher mqtt.Client, topic string, payload string, wg3 *sync.WaitGroup) {
	publisher.Publish(topic, 0, false, payload)
	wg3.Done()
}

func listen(uri string, topic string, randomId float64, wg *sync.WaitGroup, wg2 *sync.WaitGroup) {
	consumer := connect(strconv.FormatFloat(randomId, 'g', 1, 64), uri)
	consumer.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		log.Print("--- message:", string(msg.Payload()))
		wg2.Done()
	})
	wg.Done()
}

func connect(clientId string, uri string) mqtt.Client {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(uri)
	opts.SetClientID(clientId)
	tlsconf := createTlsConf()
	opts.SetTLSConfig(tlsconf)
	log.Println("------------------SetTLSConfig ok..")
	client := mqtt.NewClient(opts)
	token := client.Connect()

	for !token.WaitTimeout(3 * time.Second) {

	}

	if err := token.Error(); err != nil {
		log.Fatal(err)
	}
	return client
}


func createTlsConf() *tls.Config{
	certpool := x509.NewCertPool()
	pemCerts, err := ioutil.ReadFile("ca.pem")
	//log.Println(pemCerts)
	if err == nil {
		certpool.AppendCertsFromPEM(pemCerts)
	}
	
	cert,err := tls.LoadX509KeyPair("client-crt.pem", "client-key.pem")
	if err != nil{
		log.Println("err in load crt..",err)
	}
	//log.Println(cert)
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil{
		panic(err)
	}
	
	return &tls.Config{
		RootCAs:	certpool,
		ClientAuth:	tls.NoClientCert,
		ClientCAs:	nil,
		InsecureSkipVerify:	true,
		Certificates:	[]tls.Certificate{cert},
	}
	
}

