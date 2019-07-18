const WorthyKafkaClient = require('./worthy-kafka-client/dist/main').WorthyKafkaClient

function onMessage(key,event) {
    console.log("MAIN: got message with key " + key,event)
    WorthyKafkaClient.produce("worthy-hello-reply","reply",event,event.context_id)
}

async function run() {
    console.log("Initializing kafka client")
    await WorthyKafkaClient.init(
        ['worthy-hello-reply'],
        [{topicName:'worthy-hello',callback:onMessage}])
    console.log("Kafka client ready to receive messages")
}

run()
