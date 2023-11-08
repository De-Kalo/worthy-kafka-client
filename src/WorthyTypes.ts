
export interface IConsumerDescription {
	// topic name
	[key:string]:{
		// event name - 'default' as a wildchar - affects all event names that aren't spoken for.
		[key:string]:(message:IWorthyEvent) => void,
	}
}

export interface IProducerDescription { [key:string]:string[]}

export interface IBulkSchemaDescription {
    [key: string]: {
        [key: string]: any;
    };
}
export interface IWorthyEvent {
	topic:string						// Name of the topic to produce to
	id:string							// GUID - the event id
	key:string
	partition?:number					// kafka partition the event comes from. filled by the consumer obviously.
	eventName:string
	created:Date
	received?:Date						// filled on the consumer side upon processing - for tracking latency
	originService:string				// Origin service name.
	originServiceVersion:string			// version of the service that sent the event
	contextId:string					// operation context - used for debugging a distributed system. Events that derive
										// from the processing of other events will have the origin event id here.
	payload:any                         // application data - as received when calling the produce function.
}

export interface IWorthyKafkaClientDescription {
	consuming:IConsumerDescription,
	producing:IProducerDescription,
	bulkProducingSchema?: IBulkSchemaDescription
}
