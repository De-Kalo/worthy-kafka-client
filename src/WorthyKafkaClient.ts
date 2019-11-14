import { Kafka } from 'kafkajs'
import { v4 as uuidv4 } from 'uuid'
import Signals = NodeJS.Signals
import { HerokuKafkaCliRunner } from './HerokuKafkaCliRunner'
import { KafkaOptions, reinitEnv } from './KafkaOptions'
import { KafkaTopicManager, sleep } from './KafkaTopicManager'
import { WorthyConsumer } from './WorthyConsumer'
import { WorthyProducer } from './WorthyProducer'
import { IWorthyKafkaClientDescription } from './WorthyTypes'

export class WorthyKafkaClient {
	private _topicManager:KafkaTopicManager
	private _client:Kafka
	private _producer:WorthyProducer
	private _consumer:WorthyConsumer

	constructor() {
		/**
		 * Support termination signals
		 */
		['SIGTERM', 'SIGINT', 'SIGUSR2'].map((type) => {
			process.once(type as Signals, async () => {
				try {
					await this.shutdown()
				} finally {
					process.kill(process.pid, type)
				}
			})
		})
	}

	private async _clientSetup() {
		// Initialize the client environment. required once.
		reinitEnv()

		// When in shared kafka environment, we need to verify the consumer group first.
		if (KafkaOptions.useHerokuCli) { await this.verifyConsumerGroup() }

		// Initialize all used objects.
		this._client = new Kafka(KafkaOptions.connect)
		this._producer = new WorthyProducer()
		this._consumer = new WorthyConsumer(this._client.consumer(KafkaOptions.consumer))
		this._topicManager = new KafkaTopicManager(this._client)
	}

	// from the service perspective, the topic name is just a logical name for the topic.
	// from the infrastructure perspective, we need to support different environments on the same kafka instance,
	// as well as the KAFKA_PREFIX requested by the heroku kafka plugin - when using a shared kafka environment.
	private static _normalizeTopicName(name:string) {
		return (process.env.KAFKA_PREFIX || '') + (process.env.ENV ? process.env.ENV + '.' : '') + name
	}

	// re-generate the client description object with normalized topic names.
	private static _normalizeTopicNames(clientDescriptionIn:IWorthyKafkaClientDescription) {
		const newDescription:IWorthyKafkaClientDescription = {
				consuming:{ },
				producing:{ },
		}

		if ( clientDescriptionIn ) {
			for ( const key in clientDescriptionIn ) {
				if ( !clientDescriptionIn.hasOwnProperty(key) ) {
					continue
				}
				// type normalization for typescript compiler...
				const _key = key as 'consuming' | 'producing'
				for ( const topicName in clientDescriptionIn[key as 'consuming' | 'producing']) {
					if ( clientDescriptionIn[key as 'consuming' | 'producing'].hasOwnProperty(topicName) )  {
						const newName = WorthyKafkaClient._normalizeTopicName(topicName)
						newDescription[_key][newName] = clientDescriptionIn[_key][topicName]
					}
				}
			}
		}

		return newDescription
	}

	/**
	 * Initializes the kafka client library. must be called before first usage.
	 * Should be called just once.
	 * If a client runs the 'shutdown' command, the init command can be called again to reinitialize the client.
	 * @param clientDescriptionIn describes the topics and keys a client is planning to consume and produce.
	 */
	public async init(clientDescriptionIn:IWorthyKafkaClientDescription) {
		// basic setup of required objects.
		await this._clientSetup()

		// in a shared kafka environment, we need to normalize topic names and obfuscate this from the users.
		const clientDescription = WorthyKafkaClient._normalizeTopicNames(clientDescriptionIn)

		clientDescription.producing = clientDescription.producing || { }
		clientDescription.consuming = clientDescription.consuming || { }
		const producingTopics = Object.keys(clientDescription.producing)
		const consumingTopics = Object.keys(clientDescription.consuming)

		// initialize producer
		// first verify all producing topics exist.
		await this._topicManager.verifyTopics(producingTopics)
		// safe to initialize producer.
		await this._producer.init(this._client.producer(KafkaOptions.producer), clientDescription.producing)

		// initialize consumer
		await this._topicManager.verifyTopics(consumingTopics)
		await this._consumer.addTopics(clientDescription.consuming)

		// we need to wait for the consumer to be ready to receive messages before returning context.
		await this._consumer.waitInit()
	}

	/**
	 *
	 * @param topic the topic to produce to
	 * @param key the name of the event we want to produce (currently all event names are the topic names)
	 * @param payload application data to send with the event.
	 * @param context a context to help tracking related requests.
	 */
	public async produce(topic:string, key:string, payload:any, context?:string) {
		const nTopic = WorthyKafkaClient._normalizeTopicName(topic)
		await this._producer.produce(nTopic, key, payload, context ? context : uuidv4())
	}

	/**
	 * Creates a topic. This function is only required for test purposes. The library automatically creates topics
	 * the client declares it wants to use.
	 * @param topic the topic name to create
	 */
	public async createTopic(topic:string) {
		await this._topicManager.createTopic(process.env.ENV + '.' + topic)
	}

	/**
	 * Deletes a topic. this function is only required for test purposes. Topics should be a constant in the system
	 * and there should not be a reason (that i can think of at this point) to delete them programatically.
	 * @param topic the topic to delete
	 */
	public async deleteTopic(topic:string) {
		await this._topicManager.deleteTopic(process.env.ENV + '.' + topic)
	}

	/**
	 * Checks if a topic exists. returns boolean accordingly.
	 * @param topicName the topic name to query
	 */
	public async topicExists(topicName:string) {
		return await this._topicManager.topicExists(WorthyKafkaClient._normalizeTopicName(topicName))
	}

	/**
	 * If the consumer group this service is supposed to use doesn't exist - it will be created.
	 * This function is only relevant when using shared kafka using heroku addon. On a local environment
	 * the groups are created automatically.
	 */
	public async verifyConsumerGroup() {
		if ( !KafkaOptions.useHerokuCli ) { return }

		// when using the kafka cli we dont need kafka prefix
		const groupid = KafkaOptions.consumer.groupId.replace(process.env.KAFKA_PREFIX, '')

		// verify we don't already have a consumer group.
		if ( !HerokuKafkaCliRunner.isConsumerGroup(groupid) ) {
			// create
			HerokuKafkaCliRunner.createConsumerGroup(groupid)

			// wait until ready.
			do {
				await sleep(2000)
			} while (!HerokuKafkaCliRunner.isConsumerGroup(groupid))

			console.log('Consumer group ' + groupid + ' created.')
		} else {
			console.log('Consumer group ' + groupid + ' exists.')
		}
	}

	/**
	 * Destroys the consumer group for the current service.
	 * This function must only be called in test environments. There is no reason to programatically
	 */
	public async destroyConsumerGroup() {
		if ( KafkaOptions.useHerokuCli ) {
			HerokuKafkaCliRunner.destroyConsumerGroup(KafkaOptions.consumer.groupId)
		}
	}

	/**
	 * Shutdown the client. use before terminating the process.
	 */
	public async shutdown() {
		await Promise.all([this._consumer, this._producer, this._topicManager].map(async (obj) => {
			if ( obj ) {
				await obj.shutdown()
			}
		}))

		if ( this._client ) {
			this._client = null
		}
	}
}

export const instance = new WorthyKafkaClient()
