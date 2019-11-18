import { Producer } from 'kafkajs'
import { v4 as uuidv4 } from 'uuid'
import { WORTHY_KAFKA_CLIENT_NEW_TOPIC } from './main'
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

	public async produce(topic:string, eventName:string, payload:any, contextId:string) {
		// some basic input verifications
		if ( !this._initialized ) {
			throw new Error("Producer not yet initialized! did you call the 'init' function?")
		}

		if ( !this._supportedTopics[topic] || !this._supportedTopics[topic].includes(eventName)  ) {
			throw new Error("Unsupported topic/eventName '" + topic + '/' + eventName + "'. Known topics are: " +
							JSON.stringify(this._supportedTopics))
		}

		if ( !contextId ) {
			throw new Error('contextId argument is not defined! Must pass a contextId to the produce function.')
		}

		const eventId = uuidv4()
		if ( contextId === WORTHY_KAFKA_CLIENT_NEW_TOPIC ) {
			contextId = eventId
		}

		// Use input to construct a standard IWorthyEvent.
		const event:IWorthyEvent = {
			contextId,
			created: new Date(),
			eventName,
			id:eventId,
			key:eventName,
			originService:process.env.SERVICE_NAME,
			originServiceVersion:'v1', // TODO
			payload,
			topic,
		}

		Log.debug('Producing to topic ', topic, event)
		await this._producer.send({
			messages:[{ key:new Buffer(event.id), value:new Buffer(JSON.stringify(event))}],
			topic,
		})
	}

	public async shutdown() {
		await this._producer.disconnect()
		this._initialized = false
		this._supportedTopics = { }
	}
}
