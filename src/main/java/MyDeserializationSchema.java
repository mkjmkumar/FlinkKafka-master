import java.io.IOException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

public class MyDeserializationSchema implements DeserializationSchema<byte[]>{

	@Override
	public TypeInformation<byte[]> getProducedType() {
		return TypeInfoParser.parse("byte[]");
	}
	@Override
	public byte[] deserialize(byte[] message) throws IOException {
		return message;
	}

	@Override
	public boolean isEndOfStream(byte[] nextElement) {
		return false;
	}
}