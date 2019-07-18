const WorthyKafkaClient = require('./worthy-kafka-client/dist/main').WorthyKafkaClient

function onMessage(key,payload) {
    console.log("TEST: got message with key " + key,payload)
}

async function run() {
    console.log("TEST: Initializing kafka client")
    await WorthyKafkaClient.init(
        ['worthy-hello'],
        [{topicName:'worthy-hello-reply',callback:onMessage}]
        )
    console.log("TEST: Kafka client ready to receive messages")

    console.log("TEST: Producing message on hello-worthy")
    WorthyKafkaClient.produce("worthy-hello","Hey",{value:"Hello World"})
}

run()
