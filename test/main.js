// Importing WorthyKafkaClient
const {WorthyKafkaClient} = require('@worthy-npm/worthy-kafka-client')

// Defining a constant list of known topics for safe usage.
const KNOWN_TOPICS = {
    HELLO_WORTHY:"hello-worthy",
    HELLO_WORTHY_REPLY:"hello-worthy-reply"
}

// this is the main function that initializes the service
async function start() {
    console.log("Initializing kafka client")
    await WorthyKafkaClient.init(
        [KNOWN_TOPICS.HELLO_WORTHY_REPLY],
        {
            [KNOWN_TOPICS.HELLO_WORTHY]:{"default":onMessage}
        })
    console.log("Kafka client ready to receive messages")
}

// function to run when a message is received.
function onMessage(event) {
    console.log("Got message ",event)
    WorthyKafkaClient.produce(
        KNOWN_TOPICS.HELLO_WORTHY_REPLY,            // setting the topic to send to
        "reply",                                    // setting the key
        {                                           // setting application data.
            text:"I got a message! here it is:",
            event:event
        },
        event.contextId)                           // setting contextId as received from the event.
}

// run the service,
start()