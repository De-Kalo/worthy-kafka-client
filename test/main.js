
// Importing WorthyKafkaClient
const {WorthyKafkaClient} = require('../dist/main')
// Defining a constant list of known topics and keys for safe usage.
const KNOWN_TOPICS = {
   ITEMS:{
       _name:"items",
       ITEM_CREATED:"ITEM_CREATED",
       ESTIMATION_NEEDED:"ESTIMATION_NEEDED",
       ITEM_ESTIMATION:"ITEM_ESTIMATION"
   }
}

process.env.SERVICE_NAME="KafkaLibraryTester"
process.env.SERVICE_VERSION=1.0
process.env.HEROKU_APP_NAME="kafka-library-test-service"
process.env.ENV = 'development'
process.env.STAGE = 'development'
process.env.KAFKA_URL="localhost:9092"

// this is the main function that initializes the service
async function start() {
   console.log("Initializing kafka client")
   await WorthyKafkaClient.init({
           producing:{
               [KNOWN_TOPICS.ITEMS._name]:[KNOWN_TOPICS.ITEMS.ITEM_ESTIMATION]
           },
           consuming:{
               [KNOWN_TOPICS.ITEMS._name]:{
                  [KNOWN_TOPICS.ITEMS.ITEM_CREATED]:generateEstimation,
                  [KNOWN_TOPICS.ITEMS.ESTIMATION_NEEDED]:generateEstimation
               }
           }
       })
}

// function to run when a message is received.
function generateEstimation(event) {
   console.log("Item created! Creating estimation.",event)
   WorthyKafkaClient.produce(
       KNOWN_TOPICS.ITEMS._name,            // setting the topic to send to
       KNOWN_TOPICS.ITEMS.ITEM_ESTIMATION,  // setting the key
       {                            // setting application data.
           itemId:event.payload.itemId,
           estimation:Math.random()*10000   // generate random estimation 0-10000
                                             // (hope we do a better job in the real world)
       },
       event.contextId)                     // setting contextId as received from the event.
}
// run the service,
start()
