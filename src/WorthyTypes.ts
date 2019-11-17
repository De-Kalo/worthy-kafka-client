
export interface IConsumerDescription {
	// topic name
	[key:string]:{
		// event name - 'default' as a wildchar - affects all event names that aren't spoken for.
		[key:string]:(message:IWorthyEvent) => void,
	}
}

export interface IProducerDescription { [key:string]:string[]}

export interface IWorthyEvent {
	topic:string,
	key:string,
	id:string,                          // GUID
	eventName:string,
	created:Date,
	received?:Date,
	originService:string                // service name.
	originServiceVersion:string         // version of the service that sent the event
	contextId:string                    // operation context - it's the developer responsibility to maintain this.
	payload:any                         // application data.
}

export interface IWorthyKafkaClientDescription {
	consuming:IConsumerDescription,
	producing:IProducerDescription
}
