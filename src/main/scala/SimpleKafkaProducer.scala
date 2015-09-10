import java.util.Properties
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

class SimpleKafkaProducer {

    val topicName = "testTopic"
    println("connecting to %s".format(topicName))

    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")

    val config = new ProducerConfig(props)
    val p = new Producer[String, String](config)

	def send(message: KeyedMessage[String,String]) = p.send(message)
	def close() = p.close()
}
