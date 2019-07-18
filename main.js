const WorthyKafkaClient = require('./worthy-kafka-client/dist/main').WorthyKafkaClient

function onMessage(key,payload) {
    console.log("MAIN: got message with key " + key,payload)
}

async function run() {
    console.log("Initializing kafka client")
    WorthyKafkaClient.init(
        ['worthy-hello-reply'],
        [{topicName:'worthy-hello',callback:onMessage}])
    console.log("Kafka client ready to receive messages")
}

run()
