package demo1;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class MySource implements SourceFunction<String> {
    // 是否终止source运行
    private volatile boolean isRunning = true;
    // 随机数生成器
    private Random rand;

    public MySource() {
        this.rand = new Random();
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (this.isRunning) {
            try {
                // 生成随机a~d的用户名
                char username = 'a';
                username = (char) (username + new Random().nextInt(4));

                // 获取当前时间
                Date now = new Date( );
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                String time = dateFormat.format(now);

                // 生成一行日志
                String log = String.format("%s\t%s", String.valueOf(username), time);

                // 吐出去
                ctx.collect(log);

                // 休眠10毫秒
                Thread.sleep(10);
            } catch (Exception e) { }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
