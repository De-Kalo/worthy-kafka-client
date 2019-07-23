import { Consumer } from "kafka-node";

export type ConsumerDescription = {
    // topic name
    [key:string]:{
        // key name - 'default' as a wildchar all keys that aren't spoken for.
        [key:string]:(message:WorthyEvent) => void
    }
}

export type ProducerDescription = {[key:string]:string[]}

export interface WorthyEvent {
    topic:string,
    key:string,
    id:string,                          // GUID (?)
    created:Date,
    originService:string                // service name.
    originServiceVersion:string         // version of the service that sent the event
    contextId:string                    // operation context - it's the developer responsibility to maintain this throughout the code.
    payload:any                         // application data.
}

export interface WorthyKafkaClientDescription {
    consuming:ConsumerDescription,
    producing:ProducerDescription
}