const WorthyKafkaClient = require('../dist/main').WorthyKafkaClient

function onMessage(payload) {
    console.log("TEST: got message with key '" + payload.key + "'",payload)
}

// Defining a constant list of known topics for safe usage.
const KNOWN_TOPICS = {
    HELLO_WORTHY:"hello-worthy",
    HELLO_WORTHY_REPLY:"hello-worthy-reply"
}

async function run() {
    console.log("TEST: Initializing kafka client")
    await WorthyKafkaClient.init(
        [KNOWN_TOPICS.HELLO_WORTHY],
        {
            [KNOWN_TOPICS.HELLO_WORTHY_REPLY]: {"default": onMessage}
        })
    console.log("TEST: Kafka client ready to receive messages")

    console.log("TEST: Producing message on hello-worthy")
    WorthyKafkaClient.produce(KNOWN_TOPICS.HELLO_WORTHY,"Hey",{value:"Hello World"})
}

run()
