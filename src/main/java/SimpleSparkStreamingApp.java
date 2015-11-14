import java.util.Arrays;
import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.dstream.*;
import scala.Tuple2;
import org.apache.hadoop.mapred.TextOutputFormat;

public class SimpleSparkStreamingApp {
  public static void main(String[] args) {
    // Create a local StreamingContext with two working thread and batch interval of 1 second
    SparkConf conf = new SparkConf().setAppName("NetworkWordCount");
    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
    
    // Create a DStream that will connect to hostname:port, like localhost:9999
    JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
    
    // Split each line into words
    JavaDStream<String> words = lines.flatMap(
      new FlatMapFunction<String, String>() {
        @Override public Iterable<String> call(String x) {
          return Arrays.asList(x.split(" "));
        }
      });
    
    // Count each word in each batch
    JavaPairDStream<String, Integer> pairs = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        @Override public Tuple2<String, Integer> call(String s) {
          return new Tuple2<String, Integer>(s, 1);
        }
      });
    JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        @Override public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });
    
    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.saveAsHadoopFiles("hdfs://localhost:9000/user/spark/output/completed", "tsv", String.class, Integer.class, (Class) TextOutputFormat.class);
    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }
}
