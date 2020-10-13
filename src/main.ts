import { instance } from './WorthyKafkaClient'
export { IWorthyEvent } from './WorthyTypes'

export const WorthyKafkaClient = instance

export const WORTHY_KAFKA_CLIENT_NEW_CONTEXT_OLD = 'newtopic'
export const WORTHY_KAFKA_CLIENT_NEW_CONTEXT = 'new'
