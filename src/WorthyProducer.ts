import {KeyedMessage, Producer} from 'kafka-node'
import {WorthyEvent} from "./worthyTypes";
import {v4 as uuidv4} from 'uuid'

export class WorthyProducer {

    private _initialized:boolean=false
    private _producer:Producer
    private _supportedTopics:string[] = []

    async init(producer:Producer, registerTopics:string[]) {
        if ( this._initialized ) {
            throw new Error("Already initialized producer..")
        }
        this._producer = producer
        return new Promise(((resolve:(v?:any)=>void,reject:(v?:any)=>void) => {
            this._producer.on("ready",() => {
                this._initialized = true
                this._supportedTopics = registerTopics
                resolve()
            })
        }).bind(this))
    }

    async produce(topic:string,key:string,payload:any,contextId:string) {
        // some basic input verifications
        if ( !this._supportedTopics.includes(topic) ) {
            throw new Error("Unsupported topic '" + topic + "'. Known topics are: " + this._supportedTopics.toString() )
        }
        if ( !this._initialized ) {
            throw new Error("Producer not yet initialized! did you call the 'init' function?")
        }

        // Use input to construct a standard WorthyEvent.
        let event:WorthyEvent = {
            topic:topic,
            contextId,
            created: new Date(),
            payload:payload,
            key:key,
            id: uuidv4(),
            originService:process.env.SERVICE_NAME,
            originServiceVersion:"v1" // TODO
        }

        // return promise to support await
        return new Promise(((resolve:(v?:any)=>void,reject:(v?:any)=>void) => {
            this._producer.send([{
                topic:topic,messages:new KeyedMessage(key,JSON.stringify(event))
            }],(err:any,data:any) => {
                if ( err ) {
                    reject(err)
                }
                resolve(data)
            })
        }).bind(this))
    }
}