package eu.mjelen.flink.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

public class Feeder {

    private final StreamExecutionEnvironment env;

    public static void main(String[] args) throws Exception {
        new Feeder();
    }

    public Feeder() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");

        this.env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> words = new LinkedList<>();

        URL url = new URL("https://tools.ietf.org/rfc/rfc3501.txt");
        URLConnection conn = url.openConnection();
        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));

        String line;

        while((line = reader.readLine()) != null) {
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                words.add(word);
            }
        }

        DataStream<String> stream = this.env.fromCollection(words);

        FlinkKafkaProducer010
                .writeToKafkaWithTimestamps(stream, "words", new SimpleStringSchema(), properties);

        env.execute("Word stream feeder");
    }

}
