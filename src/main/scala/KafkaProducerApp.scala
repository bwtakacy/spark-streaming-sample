import java.util.Properties
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

class KafkaProducerApp(brokerList: String,
                       producerConfig: Properties = new Properties,
					   defaultTopic: Option[String] = None,
					   producer: Option[Producer[String, String]] = None) {

	require(brokerList == null | !brokerList.isEmpty, "Must set broker list")

	private val p = producer getOrElse {
		val effectiveConfig = {
			val c = new Properties
			c.putAll(producerConfig)
			c.put("metadata.broker.list", brokerList)
			c.put("serializer.class", "kafka.serializer.StringEncoder")
			c.put("request.required.acks", "1")
			c
		}
		new Producer[String, String](new ProducerConfig(effectiveConfig))
	}

	val config = p.config

	private def toMessage(value: String, key: Option[String] = None, topic: Option[String] = None): KeyedMessage[String, String] = {
		val t = topic.getOrElse(defaultTopic.getOrElse(throw new IllegalArgumentException("Must provide topic or default topic")))
		require(!t.isEmpty, "Topic must not be empty")
		key match {
			case Some(k) => new KeyedMessage(t, k, value)
			case _ => new KeyedMessage(t, value)
		}
	}

	def send(key: String, value: String, topic: Option[String] = None) {
		p.send(toMessage(value, Option(key), topic))
	}

	def send(value: String, topic: Option[String]) {
		send(null, value, topic)
	}

	def send(value: String, topic: String) {
		send(null, value, Option(topic))
	}

	def send(value: String) {
		send(null, value, None)
	}

	def shutdown(): Unit = p.close()

}

abstract class KafkaProducerAppFactory(brokerList: String, config: Properties, topic: Option[String] = None)
  extends Serializable {

	def newInstance(): KafkaProducerApp
}

class BaseKafkaProducerAppFactory(brokerList: String,
                                  config: Properties = new Properties,
								  defaultTopic: Option[String] = None)
  extends KafkaProducerAppFactory(brokerList, config, defaultTopic) {

	override def newInstance() = new KafkaProducerApp(brokerList, config, defaultTopic)

}
