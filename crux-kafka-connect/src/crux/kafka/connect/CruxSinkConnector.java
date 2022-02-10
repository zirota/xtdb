package crux.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CruxSinkConnector extends SinkConnector {

    public static final String URL_CONFIG = "url";
    public static final String HEADERS_CONFIG = "headers";
    public static final String HEADER_SEPARATOR_CONFIG = "header.separator";
    public static final String ID_KEY_CONFIG = "id.key";

    public static final String DEFAULT_HEADERS_SEPARATOR = "|";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(URL_CONFIG, Type.STRING, "http://localhost:3000", Importance.HIGH, "Destination URL of Crux HTTP end point.")
        .define(HEADERS_CONFIG, Type.STRING, null, Importance.LOW, "Any additional headers used to send to the url. " +
                "In the format header:value. E.g. Content-Type:application/json")
        .define(HEADER_SEPARATOR_CONFIG, Type.STRING, DEFAULT_HEADERS_SEPARATOR, Importance.LOW, "Seperator used to separate multipe headers.")
        .define(ID_KEY_CONFIG, Type.STRING, "crux.db/id", Importance.LOW, "Record key to use as :crux.db/id.");

    private String url;
    private String headers;
    private String headerSeparator;
    private String idKey;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        url = parsedConfig.getString(URL_CONFIG);
        headers = parsedConfig.getString(HEADERS_CONFIG);
        headerSeparator = parsedConfig.getString(HEADER_SEPARATOR_CONFIG);
        idKey = parsedConfig.getString(ID_KEY_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CruxSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            if (url != null)
                config.put(URL_CONFIG, url);
            if (url != null)
                config.put(ID_KEY_CONFIG, idKey);
            if (url != null)
                config.put(HEADERS_CONFIG, headers);
            if (url != null)
                config.put(HEADER_SEPARATOR_CONFIG, headerSeparator);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
