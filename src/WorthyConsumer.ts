import {Consumer, EachMessagePayload} from "kafkajs";
import {ConsumerDescription} from "./WorthyTypes";

let instance:WorthyConsumer
export class WorthyConsumer {
    private consumerInit:Promise<void>
    private readonly _consumer:Consumer
    private topicRouter:ConsumerDescription = {}

    constructor(consumer:Consumer) {
        this._consumer = consumer
        // the onMessage function cannot be bound to this, because it is bound to the kafka consumer. we need access to 'this'
        // in on message. store it in 'instance'. the WorthyKafkaClient is singleton anyway, so any proper use of the library
        // will effectively cause this class to be singleton as well.
        instance = this // the onMessage

        this.consumerInit = this._consumer.run({
            eachMessage: instance.onMessage
        })
    }

    async waitInit() {
        await this.consumerInit
        console.log("Consumer ready to receive requests")
    }

    async addTopics(topics:ConsumerDescription) {
        // return new Promise(((resolve:(v?:any) => void,reject:(v?:any)=>void) => {
        for ( let topic in topics ) {
            try {
                await this._consumer.subscribe({topic})
            } catch(err) {
                console.log("Failed subscribing to topic " + topic,err)
            }
            // adding to router. to consider - to we need to verify the function was not already registered?
            this.topicRouter[topic] = topics[topic]
        }
    }

    /**
     * kafka-node callback function - called for every message that arrives.
     * @param message
     */
    public async onMessage(payload:EachMessagePayload) {
        let message = payload.message
        let topic = payload.topic
        let router = instance.topicRouter
        // is the current topic registered? (is it possible that it isn't?(
        if ( router[topic] ) {
            try {
                // the value is expected to be a json string. if it isn't - an exception will be thrown.
                let value = message.value ? JSON.parse(message.value.toString()) : ""
                if ( typeof value === "object" ) {
                    value.received = new Date().toISOString()
                    value.topic = value.topic.replace(process.env.KAFKA_PREFIX,"").replace(process.env.ENV+".","")
                }
                // is the message key registered with a specific call function?
                if ( router[topic][message.key.toString()] ) {
                    await router[topic][message.key.toString()](value)
                } // if not - do we have a default callback for this topic?
                else if ( router[topic].default ) {
                    await router[topic].default(value)
                }
            } catch (err) {
                console.log("Error! failed processing message:",message,err)
            }
        } else {
            throw new Error("Unexpected unknown topic - " + topic + " with message:" + JSON.stringify(message))
        }
    }

    public async shutdown() {
        await this._consumer.disconnect()
    }
}