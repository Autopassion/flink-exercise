package dataStreamAPI;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * @ClassName DataStreamAPIWikiedits
 * @Description TODO
 * @Author yaoyong.fang
 * @Date 2019/10/25 14:12
 * @Version 1.0
 **/
public class DataStreamAPIWikiedits {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<WikipediaEditEvent> wikipediaEditEventDataStreamSource = environment.addSource(new WikipediaEditsSource());
        final KeyedStream<WikipediaEditEvent, String> wikipediaEditEventStringKeyedStream = wikipediaEditEventDataStreamSource.keyBy(new KeySelector<WikipediaEditEvent, String>() {
            @Override
            public String getKey(WikipediaEditEvent event){
                return event.getUser();
            }
        });
    
        final SingleOutputStreamOperator<String> result = wikipediaEditEventStringKeyedStream.timeWindow(Time.seconds(5)).aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>() {
        
            @Override
            public Tuple2<String, Long> createAccumulator() {
                return new Tuple2<>("", 0L);
            }
        
            @Override
            public Tuple2<String, Long> add(WikipediaEditEvent value, Tuple2<String, Long> accumulator) {
                accumulator.f0 = value.getUser();
                accumulator.f1 += value.getByteDiff();
                return accumulator;
            }
        
            @Override
            public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                return accumulator;
            }
        
            @Override
            public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                return new Tuple2<>(a.f0, a.f1 + b.f1);
            }
        }).map(stream -> stream.toString());
            result.print();
            result.addSink(new FlinkKafkaProducer09<>("pear-master-01.ruisdata.com:9092,pear-master-02.ruisdata.com:9092,pear-master-03.ruisdata.com:9092",
                "wiki-edits",
                new SimpleStringSchema()));
    
        environment.execute("DataStreamAPIWikiedits job");
        
    }
}
