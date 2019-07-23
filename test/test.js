
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
// this is the main function that initializes the service
async function start() {
   console.log("Initializing kafka client")
   await WorthyKafkaClient.init({
           producing:{
               [KNOWN_TOPICS.ITEMS._name]:[KNOWN_TOPICS.ITEMS.ITEM_CREATED]
           }
       })
    
    WorthyKafkaClient.produce(KNOWN_TOPICS.ITEMS._name,
        KNOWN_TOPICS.ITEMS.ITEM_CREATED,
        {
            itemId:2
        })
}

// run the service,
start()
