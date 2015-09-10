import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}

import kafka.serializer.StringDecoder
import kafka.producer.KeyedMessage

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

object KafkaSparkStreamingApp {

  def createContext(checkpointDirectory: String) : StreamingContext = {
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaSparkStreamingApp")
	sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	sparkConf.registerKryoClasses(Array(classOf[GenericObjectPool[KafkaProducerApp]]))
    val ssc = new StreamingContext(sparkConf, Seconds(2))
	ssc.checkpoint(checkpointDirectory)
	ssc
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics> <checkpointDirectory>
        |  <brokers> is a list of one or more Kafka brokers
        |  <input-topic> is a list of one or more kafka topics to consume from
		|  <output-topic> is a kafka topic to producer into
		|  <checkpointDirectory> is a path to save the checkpoint information
        |
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(brokers, inputTopics, outputTopic, checkpointDirectory) = args
	val context = StreamingContext.getOrCreate(checkpointDirectory,
	  () => {
	    createContext(checkpointDirectory)
      })

    // Create direct kafka stream with brokers and topics
    val inputTopicsSet = inputTopics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      context, kafkaParams, inputTopicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    //wordCounts.print()

	// Output to Kafka
	val producerPool = {
		val pool = createKafkaProducerPool(brokers, outputTopic)
		context.sparkContext.broadcast(pool)
	}

	wordCounts.foreachRDD(rdd => {
		rdd.foreachPartition(partitionOfRecords => {
			val p = producerPool.value.borrowObject()
			partitionOfRecords.foreach { record =>
				val now = java.time.LocalDateTime.now().toString()
				val message = "hello at " + now + record
				p.send(message)
			}
			producerPool.value.returnObject(p)
		})
	})

    // Start the computation
    context.start()
    context.awaitTermination()
  }

  private def createKafkaProducerPool(brokerList: String, topic: String): GenericObjectPool[KafkaProducerApp] = {
  	val producerFactory = new BaseKafkaProducerAppFactory(brokerList, defaultTopic = Option(topic))
	val pooledProducerFactory = new PooledKafkaProducerAppFactory(producerFactory)
	val poolConfig = {
		val c = new GenericObjectPoolConfig
		val maxNumProducers = 10
		c.setMaxTotal(maxNumProducers)
		c.setMaxIdle(maxNumProducers)
		c
	}
	new GenericObjectPool[KafkaProducerApp](pooledProducerFactory, poolConfig)
  }
}

