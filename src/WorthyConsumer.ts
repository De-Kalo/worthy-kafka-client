import {Consumer, Message} from "kafka-node";
import {ConsumerRequest, WorthyEvent} from "./worthyTypes";

let instance:WorthyConsumer
export class WorthyConsumer {
    private readonly _consumer:Consumer
    private topicRouter:ConsumerRequest = {}

    constructor(consumer:Consumer) {
        this._consumer = consumer
        this._consumer.on("message", this.onMessage)
        this._consumer.on("error", this.error)
        this._consumer.on("offsetOutOfRange",this.offsetOutOfRange)

        // the onMessage function cannot be bound to this, because it is bound to the kafka consumer. we need access to 'this'
        // in on message. store it in 'instance'. the WorthyKafkaClient is singleton anyway, so any proper use of the library
        // will effectively cause this class to be singleton as well.
        instance = this // the onMessage
    }

    addTopics(topics:ConsumerRequest) {
        return new Promise(((resolve:(v?:any) => void,reject:(v?:any)=>void) => {
            this._consumer.addTopics(Object.keys(topics),(err:any,added:string[]) => {
                if ( err ) {
                    console.log("Error!",err)
                    throw new Error("Failed registering topics for consume! ")
                }
                // add all topics to router
                for ( let toAdd in topics ) {
                    // make sure this topic was successfully added
                    if ( added.includes(toAdd) ) {
                        // adding to router. to consider - to we need to verify the function was not already registered?
                        this.topicRouter[toAdd] = topics[toAdd]
                    } else {
                        // this topic was not added for some reason. notify!
                        console.log("Error! failed adding " + toAdd + " to consume list")
                    }
                }
                resolve()
            })
        }).bind(this))
    }

    /**
     * kafka-node callback function - called for every message that arrives.
     * @param message
     */
    onMessage(message:Message) {
        let router = instance.topicRouter
        // is the current topic registered? (is it possible that it isn't?(
        if ( router[message.topic] ) {
            try {
                // the value is expected to be a json string. if it isn't - an exception will be thrown.
                let value = message.value ? JSON.parse(message.value.toString()) : ""
                // is the message key registered with a specific call function?
                if ( router[message.topic][<string>message.key] ) {
                    router[message.topic][<string>message.key](value)
                } // if not - do we have a default callback for this topic?
                else if ( router[message.topic].default ) {
                    router[message.topic].default(value)
                }
            } catch (err) {
                console.log("Error! failed processing message:",message,err)
            }
        } else {
            throw new Error("Unexpected unknown topic - " + message.topic + " with message:" + JSON.stringify(message))
        }
    }

    error(error:any) {
        console.log("Error!",error)
    }

    offsetOutOfRange(error:any) {
        console.log("Offset out of range! ",error)
    }
}