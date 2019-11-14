
export interface IConsumerDescription {
	// topic name
	[key:string]:{
		// key name - 'default' as a wildchar all keys that aren't spoken for.
		[key:string]:(message:IWorthyEvent) => void,
	}
}

export interface IProducerDescription { [key:string]:string[]}

export interface IWorthyEvent {
	topic:string,
	key:string,
	id:string,                          // GUID (?)
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
