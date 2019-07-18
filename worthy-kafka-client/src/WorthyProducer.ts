import {KeyedMessage, Producer} from 'kafka-node'
import {WorthyEvent} from "./WorthyEvent";

export class WorthyProducer {

    initialized:boolean=false
    producer:Producer
    supportedTopics:string[] = []

    async init(producer:Producer, supportedTopics:string[]) {
        if ( this.initialized ) {
            throw new Error("Already initialized producer..")
        }
        this.producer = producer
        return new Promise(((resolve:(v?:any)=>void,reject:(v?:any)=>void) => {
            this.producer.on("ready",() => {
                this.initialized = true
                this.supportedTopics = supportedTopics
                resolve()
            })
        }).bind(this))
    }

    async produce(topicName:string,key:string,payload:WorthyEvent) {
        if ( !this.supportedTopics.includes(topicName) ) {
            throw new Error("Unsupported topic '" + topicName + "'. Known topics are: " + this.supportedTopics.toString() )
        }

        return new Promise(((resolve:(v?:any)=>void,reject:(v?:any)=>void) => {
            this.producer.send([{
                topic:topicName,messages:new KeyedMessage(key,JSON.stringify(payload))
            }],(err:any,data:any) => {
                if ( err ) {
                    reject(err)
                }
                resolve(data)
            })
        }).bind(this))
    }
}