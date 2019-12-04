import { Consumer, ConsumerEvents, EachMessagePayload } from '@worthy-npm/kafkajs-worthy-copy'
import { IConsumerDescription, IWorthyEvent } from './WorthyTypes'

import { getLog } from '@worthy-npm/worthy-logger'
const Log = getLog('WorthyKafkaClient')

let instance:WorthyConsumer
export class WorthyConsumer {
	private readonly _consumer:Consumer
	private topicRouter:IConsumerDescription = { }
	private currentContextId:string

	constructor(consumer:Consumer) {
		this._consumer = consumer
		instance = this
	}

	public async waitInit() {
		await this._consumer.run({
			eachMessage: instance.onMessage,
		})
		// await this.consumerInit
		Log.debug('Consumer ready to receive requests')
	}

	public async addTopics(topics:IConsumerDescription) {
		// return new Promise(((resolve:(v?:any) => void,reject:(v?:any)=>void) => {
		for ( const topic in topics ) {
			if ( !topics.hasOwnProperty(topic) ) {
				continue
			}
			try {
				Log.debug('Subscribing to topic', topic )
				await this._consumer.subscribe({ topic})
			} catch (err) {
				Log.error('Failed subscribing to topic ' + topic, err)
			}
			// adding to router. to consider - to we need to verify the function was not already registered?
			this.topicRouter[topic] = topics[topic]
		}
	}

	/**
	 * kafka-node callback function - called for every message that arrives.
	 * @param payload - the message payload.
	 */
	public async onMessage(payload:EachMessagePayload) {
		const time = new Date().getTime()
		const message = payload.message
		const topic = payload.topic
		const router = instance.topicRouter
		Log.debug({ message:'Got message', topic, key: message.key.toString() })

		// is the current topic registered? (is it possible that it isn't?(
		if ( router[topic] ) {
			try {
				// the value is expected to be a json string. if it isn't - an exception will be thrown.
				const value:IWorthyEvent = message.value ? JSON.parse(message.value.toString()) : ''
				if ( typeof value === 'object' ) {
					value.received = new Date()
					value.topic = value.topic.replace(process.env.KAFKA_PREFIX, '').replace(process.env.ENV + '.', '')
				}

				instance.setCurrentContextId(value)

				// TODO: supporting the old 'key' key alongside the 'eventName' key. after transition ends delete the old.
				const eventName = (value.eventName || value.key).toString()
				let callback:(event:IWorthyEvent) => void = null
				let callbackName:string = ''

				// is the message event name registered with a specific call function?
				if ( router[topic][eventName] ) {
					callbackName = router[topic][eventName].name
					callback = router[topic][eventName]
				} else if ( router[topic].default ) {
					callbackName = 'default'
					callback = router[topic].default
				}

				if ( callback ) {
					Log.info(`Processing event ${eventName} with callback: ${callbackName}`)
					Log.debug('Message payload:', value)
					await callback(value)
					Log.info(`${eventName} processing done. Duration: ${new Date().getTime() - time} ms`)
				} else {
					Log.debug(`${eventName} no callback function. Skipping.`)
				}
			} catch (err) {
				Log.error('Error! failed processing message:', message, err)
			} finally {
				instance.resetMetadata()
			}
		} else {
			Log.debug('Got message from unexpected topic ' + topic)
			throw new Error('Unexpected unknown topic - ' + topic + ' with message:' + JSON.stringify(message))
		}
	}

	public getCurrentContext() {
		return this.currentContextId
	}

	private setCurrentContextId(value:IWorthyEvent) {
		if (process.env.WORTHY_KAFKA_CLIENT_AUTO_SET_CONTEXT === 'true') {
			// verify we don't have a context leakage.
			if (value.contextId && this.currentContextId) {
				throw new Error(`UNEXPECTED ERROR: currentContextId already exists! current value is: ${this.currentContextId}`)
			}
			this.currentContextId = value.contextId
			getLog().setMetadata('eventContext', value.contextId)
			getLog().setMetadata('currentEvent', value.eventName)
		}
	}

	private resetMetadata() {
		if (process.env.WORTHY_KAFKA_CLIENT_AUTO_SET_CONTEXT === 'true') {
			this.currentContextId = null
			getLog().setMetadata('eventContext', null)
			getLog().setMetadata('currentEvent', null)
		}
	}

	public async shutdown() {
		await this._consumer.disconnect()
	}

	public trackConsumerEvents(events:string[]) {
		events.forEach((e) => {
			if ( this._consumer.events.hasOwnProperty(e) ) {
				this._consumer.on(this._consumer.events[e as keyof ConsumerEvents], (ce) => Log.debug(ce))
			} else {
				Log.warn('Requested to track non-existing consumer event ' + e)
			}
		})
	}
}
