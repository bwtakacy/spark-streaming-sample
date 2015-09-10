import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.{PooledObject, BasePooledObjectFactory}

class PooledKafkaProducerAppFactory(val factory: KafkaProducerAppFactory)
  extends BasePooledObjectFactory[KafkaProducerApp] with Serializable {

	override def create(): KafkaProducerApp = factory.newInstance()

	override def wrap(obj: KafkaProducerApp): PooledObject[KafkaProducerApp] = new DefaultPooledObject(obj)

	override def destroyObject(p: PooledObject[KafkaProducerApp]): Unit = {
		p.getObject.shutdown()
		super.destroyObject(p)
	}
}
