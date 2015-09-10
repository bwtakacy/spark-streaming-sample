import kafka.producer.{KeyedMessage}

class KafkaSink(createProducer: () => SimpleKafkaProducer)
  extends Serializable {

	lazy val producer = createProducer()

	def send(message: KeyedMessage[String,String]) = producer.send(message)
}

object KafkaSink {
	def apply(): KafkaSink = {
		val f = () => {
			val producer = new SimpleKafkaProducer()

			sys.addShutdownHook {
				producer.close()
			}
		    
			producer
		}
		new KafkaSink(f)
	}
}
