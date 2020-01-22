package demo1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MyMapper implements FlatMapFunction<String, Tuple2<String, Long>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
        // 按\t分割
        String []fields = value.split("\t");
        String username = fields[0];
        String time = fields[1];

        // 解析时间为unix时间戳
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date date = dateFormat.parse(time);
        Long ts = date.getTime();    // flink的时间都是毫秒

        // 提交tuple
        out.collect(new Tuple2<>(username, ts));
    }
}
