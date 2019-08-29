import {Producer} from 'kafkajs'
import {WorthyEvent} from "./WorthyTypes";
import {v4 as uuidv4} from 'uuid'
import { ProducerDescription } from './WorthyTypes';
import { Logger } from './Logger';

export class WorthyProducer {

    private _initialized:boolean=false
    private _producer:Producer
    private _supportedTopics:ProducerDescription = {}

    async init(producer:Producer, registerTopics:ProducerDescription) {
        if ( this._initialized ) {
            throw new Error("Already initialized producer..")
        }
        this._producer = producer
        await this._producer.connect()
        this._initialized = true
        // clone input
        for ( let topic in registerTopics) {
            this._supportedTopics[topic] = registerTopics[topic].slice(0)
        }
    }

    async produce(topic:string,key:string,payload:any,contextId:string) {
        // some basic input verifications
        if ( !this._supportedTopics[topic] || !this._supportedTopics[topic].includes(key)  ) {
            throw new Error("Unsupported topic/key '" + topic + "/"+key+"'. Known topics are: " + 
                            JSON.stringify(this._supportedTopics))
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

        Logger.debug("Producing to topic ",topic)
        await this._producer.send({
            topic:topic,
            messages:[{key:new Buffer(key),value:new Buffer(JSON.stringify(event))}]
        })
    }

    public async shutdown() {
        await this._producer.disconnect()
        this._initialized = false
        this._supportedTopics = {}
    }
}