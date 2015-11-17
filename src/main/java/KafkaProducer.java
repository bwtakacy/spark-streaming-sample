
import java.util.Properties;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer extends Thread {

  private kafka.javaapi.producer.Producer<Integer, String> producer;
  private String topic;
  private Properties props = new Properties();

  public KafkaProducer(String brokers, String topic) {
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", brokers);
    producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
    this.topic = topic;
  }

  public void run() {
    int messageNo = 1;
    while(true)
    {
      String messageStr = new String("Message: " + messageNo);
      producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
      messageNo++;
      try {
        sleep(100);
      } catch (InterruptedException e) {}
    }
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: KafkaProducer <brokers> <topic>\n" +
          "  <brokers> is a list of one or more Kafka brokers\n" +
          "  <topic> is a kafka topic to produce to\n\n");
      System.exit(1);
    }
    String brokers = args[0];
    String topic = args[1];
    KafkaProducer producerThread = new KafkaProducer(brokers, topic);
    producerThread.start();
  }
}
