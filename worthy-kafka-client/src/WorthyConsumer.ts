import {Consumer, Message} from "kafka-node";
import {ConsumeRequest} from "./ConsumeRequest";
import {WorthyEvent} from "./WorthyEvent";

export class WorthyConsumer {
    private readonly _consumer:Consumer
    private topicRouter:{[key:string]:(key:string,payload:WorthyEvent) => void} = {}

    constructor(consumer:Consumer) {
        this._consumer = consumer
        this._consumer.on("message", this.onMessage)
        this._consumer.on("error", this.error)
        this._consumer.on("offsetOutOfRange",this.offsetOutOfRange)
    }

    addTopics(topics:ConsumeRequest[]) {
        return new Promise(((resolve:(v?:any) => void,reject:(v?:any)=>void) => {
            this._consumer.addTopics(topics.map(t => t.topicName),(err:any,added:string[]) => {
                if ( err ) {
                    console.log("Error!",err)
                    throw new Error("Failed registering topics for consume! ")
                }
                // add all topics to router
                for ( let toAdd of topics ) {
                    // make sure this topic was successfully added
                    if ( added.includes(toAdd.topicName) ) {
                        // adding to router. to consider - to we need to verify the function was not already registered?
                        this.topicRouter[toAdd.topicName] = toAdd.callback
                    } else {
                        // this topic was not added for some reason. notify!
                        console.log("Error! failed adding " + toAdd.topicName + " to consume list")
                    }
                }
                resolve()
            })
        }).bind(this))
    }

    onMessage(message:Message) {
        if ( this.topicRouter[message.topic] ) {
            try {
                let value = message.value ? JSON.parse(message.value.toString()) : ""
                this.topicRouter[message.topic](message.key ? message.key.toString() : null, value)
            } catch (err) {
                console.log("Error! failed processing message:",message,err)
            }
        }
    }

    error(error:any) {
        console.log("Error!",error)
    }

    offsetOutOfRange(error:any) {
        console.log("Offset out of range! ",error)
    }
}