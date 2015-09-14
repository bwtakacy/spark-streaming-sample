/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import kafka.serializer.StringDecoder
import kafka.producer.KeyedMessage

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

object ImprovedDirectKafkaWordCount {
  def createContext(checkpointDirectory: String) : StreamingContext = {
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
	ssc.checkpoint(checkpointDirectory)
	ssc
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics> <checkpointDirectory>
        |  <brokers> is a list of one or more Kafka brokers
        |  <inputTopics> is a list of one or more kafka topics to consume from
        |  <outputTopic> is a  kafka topics to produce into
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
    //val words = lines.flatMap(_.split(" "))
    val wordCounts = lines.map(x => (x, 1L)).reduceByKey(_ + _)
    //wordCounts.print()

	// Output to Kafka
	val kafkaSink = context.sparkContext.broadcast(KafkaSink())
	wordCounts.foreachRDD(rdd => {
		rdd.foreach(record => {
			val message = "The message read from Kakfa: " + record
			kafkaSink.value.send(new KeyedMessage[String, String](outputTopic, "one", message))
		})
	})

	// Setup gracefull stop
	sys.ShutdownHookThread {
		System.err.println("Gracefully stopping Spark Streaming Application ")
		context.stop(true, true)
		System.err.println("Application stopped ")
	}

    // Start the computation
    context.start()
    context.awaitTermination()
  }
}
