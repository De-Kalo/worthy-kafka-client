
export class Logger {
	public static debug(...args:any[]) {
		if ( process.env.WORTHY_KAFKA_CLIENT_DEBUG ) {
			this.log('DEBUG', args)
		}
	}
	public static info(...args:any[]) {
		if ( process.env.WORTHY_KAFKA_CLIENT_DEBUG ) {
			this.log('DEBUG', args)
		}
	}
	private static log(level:string, ...argsIn:any[]) {
		const args = ['\x1b[36m[WorthyKafkaLibrary] [' + level + ']\x1b[0m  |  '].concat(argsIn[0])
		console.log.apply(console, args)
	}
}

if ( process.env.WORTHY_KAFKA_CLIENT_DEBUG ) {
	Logger.info('Starting in debug mode')
} else {
	Logger.info('Logger deactivated')
}
