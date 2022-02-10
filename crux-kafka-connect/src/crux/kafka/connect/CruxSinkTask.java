package crux.kafka.connect;

import clojure.lang.Keyword;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import crux.api.Crux;
import crux.api.ICruxAPI;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;


public class CruxSinkTask extends SinkTask {
    private Map<String,String> props;
    private Closeable api;
    private static IFn submitSinkRecords;

    static {
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.kafka.connect"));
        submitSinkRecords = Clojure.var("crux.kafka.connect/submit-sink-records");
    }

    @Override
    public String version() {
        return new CruxSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        this.api = (Closeable) Clojure.var("crux.api/new-api-client").invoke(props.get(CruxSinkConnector.URL_CONFIG));
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        submitSinkRecords.invoke(props, sinkRecords);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void stop() {
        if (api != null)
            try {
                api.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
    }
}
