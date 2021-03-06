import { getLog } from '@worthy-npm/worthy-logger'
import { execSync } from 'child_process'
import { KafkaOptions } from './KafkaOptions'
const Log = getLog('WorthyKafkaClient', undefined, process.env.WORTHY_KAFKA_CLIENT_LOG_LEVEL || 'info')

export class HerokuKafkaCliRunner {

	public static runCmd(cmd:string, confirm:boolean = false) {
		const str = 'heroku kafka:' + cmd + ' -a ' + process.env.HEROKU_APP_NAME +
			(confirm ? ' --confirm ' + process.env.HEROKU_APP_NAME : '')
		Log.debug({ message:'Running CLI command', command:'str' })
		return execSync(str)
	}

	public static createTopic(topic:string) {
		this.runCmd('topics:create ' + topic +
			' --partitions ' + KafkaOptions.topic.partitions +
			' --replication-factor ' + KafkaOptions.topic.replication)
	}

	public static deleteTopic(topic:string) {
		this.runCmd('topics:destroy ' + topic, true)
	}

	public static topicInfo(topic:string) {
		return this.runCmd('topics:info ' + topic)
	}

	public static isConsumerGroup(group:string) {
		const consumerGroups = this.runCmd('consumer-groups')
		return consumerGroups.toString().includes(group)
	}

	public static createConsumerGroup(group:string) {
		this.runCmd('consumer-groups:create ' + group)
	}

	public static destroyConsumerGroup(group:string) {
		this.runCmd('consumer-groups:destroy ' + group, true)
	}
}
