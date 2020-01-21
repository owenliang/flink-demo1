package demo1;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import javax.xml.crypto.Data;
import java.text.SimpleDateFormat;

public class MyStreaming {
  public static void main(String[] args) throws Exception {
    // 获取flink集群环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 新建source
    MySource source = new MySource();
    // 添加到env, 得到一个DataStream
    DataStream<String> sourceStream = env.addSource(source);

    // 解析日志为若2元组
    DataStream<Tuple2<String,Long>> tupleStream = sourceStream.flatMap(new MyMapper());

    // 设置按照event time计算日志落入哪个window
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // 定义如何为每条日志设置event time，以及基于日志的时间来更新watermark以便让过期窗口进行计算。
    DataStream<Tuple2<String,Long>> timedStream = tupleStream.assignTimestampsAndWatermarks(new MyAssigner());

    // 按tuple的第0列分组
    KeyedStream<Tuple2<String,Long>, Tuple> keyedStream = timedStream.keyBy(0);

    // 按eventime 5秒划分window，产生带边界的数据流
    WindowedStream<Tuple2<String,Long>, Tuple, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

    // 聚合统计
    DataStream<Tuple2<String, Integer>> aggStream = windowStream.aggregate(new MyAgg());

    // 输出结果
    aggStream.addSink(new SinkFunction<Tuple2<String, Integer>>() {
      @Override
      public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        System.out.println(value);  // 仅仅是打印一下
      }
    });

    // 提交
    env.execute("demo1");
  }
}
