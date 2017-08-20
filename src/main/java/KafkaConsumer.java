//Flink Consumer on a Kafka Topic, Mean pass a Kafka producer and this consumer will display consumed values from Kafka
import java.util.Properties;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Created by Mukesh on 2017/07/21.
 */
public class KafkaConsumer {

    public static String BOOTSTRAP_SERVER = "victoria.com:6667";
    public static String TOPIC = "mukeshtopic";
    public static String GROUP_ID = "test";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVER);
        props.setProperty("group.id", GROUP_ID);

        FlinkKafkaConsumer010<String> consumer =
                new FlinkKafkaConsumer010<>(TOPIC, new SimpleStringSchema(), props);
        consumer.setStartFromEarliest();

        env.addSource(consumer).print();
        env.execute();
    }
}
