package eu.mjelen.flink.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.*;

public class Main {

    private final StreamExecutionEnvironment env;

    public static void main(String[] args) throws Exception {
        new Main();
    }

    public Main() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");

        this.env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream =
                this.env.addSource(new FlinkKafkaConsumer010<>("words", new SimpleStringSchema(), properties));

        DataStream<String> parsed = stream.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> collector) throws Exception {
                collector.collect(new WordCount(value, 1L));
            }
        }).keyBy("word")
        .reduce(new ReduceFunction<WordCount>() {
            @Override
            public WordCount reduce(WordCount a, WordCount b) throws Exception {
                return new WordCount(a.getWord(), a.getCount() + b.getCount());
            }
        }).flatMap(new FlatMapFunction<WordCount, String>() {
                    @Override
                    public void flatMap(WordCount value, Collector<String> collector) throws Exception {
                        collector.collect(value.toString());
                    }
                });

        FlinkKafkaProducer010
                .writeToKafkaWithTimestamps(parsed, "word-counts", new SimpleStringSchema(), properties);

        env.execute("WordCount example");
    }

    public static class WordCount {

        public String word;
        public long count;

        public WordCount() {
        }

        public WordCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return word + ":" + count;
        }

    }
}
