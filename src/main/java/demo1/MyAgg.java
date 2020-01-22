package demo1;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MyAgg implements AggregateFunction<Tuple2<String,Long>, Tuple2<String,Integer>, Tuple2<String,Integer>> {

    @Override
    public Tuple2<String, Integer> createAccumulator() {
        return new Tuple2<>("",0 ); // 窗口计算前初始值
    }

    // 窗口迭代计算
    @Override
    public Tuple2<String, Integer> add(Tuple2<String, Long> value, Tuple2<String, Integer> accumulator) {
        // 迭代一条记录
        accumulator.f0 = value.f0; // 设置username
        accumulator.f1 += 1; //  累计访问次数
        // System.out.println(accumulator);
        return accumulator;
    }

    // 返回最终结果
    @Override
    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
        // System.out.println(accumulator);
        return accumulator;
    }

    // 合并并发计算的2个中间结果
    @Override
    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
        Tuple2<String, Integer> merged = new Tuple2<String, Integer>();
        merged.f0 = a.f0;
        merged.f1 = a.f1 + b.f1;
        return merged;
    }
}
