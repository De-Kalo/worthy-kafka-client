export interface WorthyEvent {
    topic:string,
    key:string,
    id:string,      // GUID (?)
    created_timestamp:Date,
    origin_service:string               // service name.
    origin_service_version:string       // version of the service that sent the event
    origin_machine_id:string            // identifier of the machine that send the event.
    context_id:string                   // operation context - it's the developer responsibility to maintain this throughout the code.
    payload:any                         //
}