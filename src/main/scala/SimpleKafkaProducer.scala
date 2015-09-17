import java.util.Properties
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

class SimpleKafkaProducer (brokers: String) {

    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    props.put("message.send.max.retries", "100000")
    props.put("retry.backoff.ms", "1000")

    val config = new ProducerConfig(props)
    val p = new Producer[String, String](config)

	def send(message: KeyedMessage[String,String]) = p.send(message)
	def close() = p.close()
}
