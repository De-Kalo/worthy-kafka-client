import { Producer } from 'kafkajs'
import { v4 as uuidv4 } from 'uuid'
import { IWorthyEvent } from './WorthyTypes'
import { IProducerDescription } from './WorthyTypes'

import { getLog } from '@worthy-npm/worthy-logger'
const Log = getLog('WorthyKafkaClient')

export class WorthyProducer {

	private _initialized:boolean=false
	private _producer:Producer
	private _supportedTopics:IProducerDescription = { }

	public async init(producer:Producer, registerTopics:IProducerDescription) {
		if ( this._initialized ) {
			throw new Error('Already initialized producer..')
		}
		this._producer = producer
		await this._producer.connect()
		this._initialized = true
		// clone input
		for ( const topic in registerTopics) {
			if ( registerTopics.hasOwnProperty(topic) ) {
				this._supportedTopics[topic] = registerTopics[topic].slice(0)
			}
		}
	}

	public async produce(topic:string, key:string, payload:any, contextId:string) {
		// some basic input verifications
		if ( !this._supportedTopics[topic] || !this._supportedTopics[topic].includes(key)  ) {
			throw new Error("Unsupported topic/key '" + topic + '/' + key + "'. Known topics are: " +
							JSON.stringify(this._supportedTopics))
		}
		if ( !this._initialized ) {
			throw new Error("Producer not yet initialized! did you call the 'init' function?")
		}

		// Use input to construct a standard IWorthyEvent.
		const event:IWorthyEvent = {
			contextId,
			created: new Date(),
			id: uuidv4(),
			key,
			originService:process.env.SERVICE_NAME,
			originServiceVersion:'v1', // TODO
			payload,
			topic,
		}

		Log.debug('Producing to topic ', topic)
		await this._producer.send({
			messages:[{ key:new Buffer(key), value:new Buffer(JSON.stringify(event))}],
			topic,
		})
	}

	public async shutdown() {
		await this._producer.disconnect()
		this._initialized = false
		this._supportedTopics = { }
	}
}
