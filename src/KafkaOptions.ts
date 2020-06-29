import { getLog, reinitLog } from '@worthy-npm/worthy-logger'
import { ConsumerConfig, KafkaConfig, logLevel, ProducerConfig } from 'kafkajs'
const Log = getLog('WorthyKafkaClient', undefined, process.env.WORTHY_KAFKA_CLIENT_LOG_LEVEL || 'info')

interface IKafkaOptions  {
	connect:KafkaConfig
	consumer:ConsumerConfig
	producer:ProducerConfig
	topic:{
		replication:number,
		partitions:number,
	},
	useHerokuCli?:boolean
}

const options:IKafkaOptions = {
	connect:{
		brokers:[],
		connectionTimeout: 5000,
		logCreator: (inlevel:logLevel) => ({ namespace, level, label, log}) => {
			const translate = {
				[logLevel.ERROR]:'error',
				[logLevel.WARN]:'warning',
				[logLevel.INFO]:'info',
				[logLevel.DEBUG]:'debug',
			}
			const offset = namespace === 'Connection' ? 3 : 2
			// @ts-ignore
			const logger = reinitLog(`KafkaJs:${namespace}`, translate[inlevel], false, offset)
			switch ( level ) {
				case logLevel.DEBUG:
					logger.debug(log)
					break
				case logLevel.INFO:
					logger.info(log)
					break
				case logLevel.WARN:
					logger.warn(log)
					break
				case logLevel.ERROR:
					logger.error(log)
			}
		},
		requestTimeout: 5000,
	},
	consumer:{
		allowAutoTopicCreation:false,
		groupId: process.env.SERVICE_NAME + '-' + process.env.ENV,
		heartbeatInterval:10000,
		maxInFlightRequests:5,
		rebalanceTimeout:90000,
		sessionTimeout:60000,
	},
	producer:{
		allowAutoTopicCreation: false,
		idempotent: false,
		maxInFlightRequests: 5,
	},
	topic: {
		partitions:1,
		replication:3,
	},
}

/**
 * Run on initialization to verify all required environment variables are accounted for.
 */
function verifyEnvironment() {
	// The following variables are required for properly running the library.
	// check that all exist.
	const requiredVariables:any = {
		ENV:'The environment name. This is different from STAGE as there can be several qa or development environments.',
		KAFKA_URL:"A list of comma separated values of 'host:port' strings representing kafka brokers.",
		STAGE:'The stage of the current service (production / qa / development...)',
	}

	// when not in development we need ssl certification for kafka, and make sure the app name is available.
	if ( process.env.STAGE && process.env.STAGE !== 'development' ) {
		requiredVariables.KAFKA_CLIENT_CERT_KEY = 'A certification key for ssl connection. provided by heroku kafka plugin in non development environments.'
		requiredVariables.KAFKA_CLIENT_CERT = 'A certification for ssl connection. provided by heroku kafka plugin in non development environments.'
		requiredVariables.KAFKA_TRUSTED_CERT = 'A certificate authorization certification for ssl connection. profided by heroku kafka plugin in non development environment.'
		requiredVariables.HEROKU_APP_NAME = 'An environment variable containing the current app name. Provided by the heroku:labs dyno-runtime-metadata setting.'
	}

	// go over required environment and make sure it exists.
	for ( const key in requiredVariables ) {
		if ( requiredVariables.hasOwnProperty(key) ) {
			if ( process.env[key] === undefined ) {
				throw new Error('Cannot find environment variable: ' + key + '. Variable description:\n\t' + requiredVariables[key])
			}
		}
	}
}

// required for clients that load process env after process is loaded.
export function reinitEnv() {
	// some sanity checks
	verifyEnvironment()

	// change options by environment.
	switch ( process.env.STAGE ) {
		case 'production':
		case 'qa':
			options.topic.replication = 3
			options.topic.partitions = 8
			break
		case 'development':
			options.topic.replication = 1
			options.topic.partitions = 1
	}

	// heroku kafka add on adds irrelevant prefix to the urls. fix it.
	const kafkaUrls = process.env.KAFKA_URL.replace(/kafka\+ssl:\/\//g, '')
	options.connect.brokers = kafkaUrls.split(',')

	if ( process.env.KAFKA_CLIENT_CERT_KEY && process.env.KAFKA_CLIENT_CERT ) {
		options.connect.ssl = {
			ca:[process.env.KAFKA_TRUSTED_CERT],
			cert:process.env.KAFKA_CLIENT_CERT,
			key:process.env.KAFKA_CLIENT_CERT_KEY,
			rejectUnauthorized: false,
		}
	}

	options.consumer.groupId = (process.env.KAFKA_PREFIX || '') + process.env.SERVICE_NAME + '-' + process.env.ENV
	options.useHerokuCli = process.env.ENV !== 'development'

	Log.debug({ message:'Initializing kafka client environment', options })
}
export const KafkaOptions:IKafkaOptions = options
