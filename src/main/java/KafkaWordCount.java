import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Lists;
import kafka.serializer.StringDecoder;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 */

public final class KafkaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) {
    if (args.length < 3) {
      System.err.println("Usage: DirectKafkaWordCount <brokers> <topics> <outputPath> [<batchInterval>]\n" +
          "  <brokers> is a list of one or more Kafka brokers\n" +
          "  <topics> is a list of one or more kafka topics to consume from\n" +
          "  <outputPath> is a HDFS path to output files\n" +
          "  <batchInterval> is a integer value of duration of streaming job\n\n");
      System.exit(1);
    }

    Logger.getRootLogger().setLevel(Level.WARN);

    String brokers = args[0];
    String topics = args[1];
    String outputPath = args[2];
    int batchInterval = 10;
    if (args.length == 4) {
        batchInterval = Integer.parseInt(args[4]);
    }

    // Create context with 2 second batch interval
    SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchInterval));

    HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list", brokers);

    // Create direct kafka stream with brokers and topics
    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
        jssc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        kafkaParams,
        topicsSet
    );

    // Get the lines, split them into words, count the words and print
    JavaDStream<String> lines = messages.map(tuple2 -> tuple2._2());
    JavaDStream<String> words = lines.flatMap(line -> Lists.newArrayList((SPACE.split(line))));
    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1)).reduceByKey((x, y) -> x + y);

    // output to HDFS
    wordCounts.saveAsNewAPIHadoopFiles(outputPath, "", String.class, Integer.class, (Class) TextOutputFormat.class);

    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }
}
