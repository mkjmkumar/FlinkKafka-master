//Flink Run a Kafka Sink from one topic to another
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class KafkaSink {

	public static <T> void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        //env.enableCheckpointing(1000);
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "victoria.com:6667");
        properties.setProperty("zookeeper.connect", "victoria.com:2181");
        properties.setProperty("group.id", "test");
        properties.setProperty("auto.offset.reset", "earliest"); 
        
        MyDeserializationSchema myDeserializationSchema = new MyDeserializationSchema();
        DataStreamSource<byte[]> stream = (DataStreamSource<byte[]>) env.addSource(new FlinkKafkaConsumer09<>("imagetext", (DeserializationSchema<T>)myDeserializationSchema, properties));
        
        SingleOutputStreamOperator<String> stringStream = stream.map(new MapFunction<byte[], String>() {

			@Override
			public String map(byte[] value) throws Exception {
				return new String(value);
			}
		});
        stringStream.addSink(new FlinkKafkaProducer09<>("victoria.com:6667","imagetextgroup",new SimpleStringSchema()));
        env.execute("Flink Kafka Sink tester");
	}
}