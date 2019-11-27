process.env.SERVICE_NAME="KafkaLibraryTester3"
process.env.SERVICE_VERSION=1.0
process.env.HEROKU_APP_NAME="kafka-library-test-service"
process.env.ENV = 'development'
process.env.STAGE = 'development'
process.env.KAFKA_URL="localhost:9092"

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


// this is the main function that initializes the service
async function start() {
   console.log("Initializing kafka client")
   await WorthyKafkaClient.init({
           producing:{
               [KNOWN_TOPICS.ITEMS._name]:[KNOWN_TOPICS.ITEMS.ITEM_CREATED]
           }
       })

	const itemId = Math.floor(Math.random()*10000)
    await WorthyKafkaClient.produce(KNOWN_TOPICS.ITEMS._name,
        KNOWN_TOPICS.ITEMS.ITEM_CREATED,
        {
            itemId
        }, ""+itemId)

}

// run the service,
start()
