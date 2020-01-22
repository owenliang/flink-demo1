package demo1;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class MyAssigner implements AssignerWithPunctuatedWatermarks<Tuple2<String, Long>> {

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Tuple2<String, Long> lastElement, long extractedTimestamp) {
        // System.out.println(extractedTimestamp);
        return new Watermark(extractedTimestamp - 2000);   // 基于该日志时间倒退2秒作为watermark
    }

    @Override
    public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
        // System.out.println(element);
        return element.f1;  // 日志时间作为event time
    }
}
