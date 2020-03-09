package com.atguigu.apitest.sinkTest.hdfs;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

public class XXX {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String outputPath = "/Users/houningning/bak/flink/aaa";
        env.setParallelism(1);
        env.enableCheckpointing(5000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.of(10L, TimeUnit.SECONDS)));

        /*final StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(
                new Path(outputPath),
                new SimpleStringEncoder<String>())
                .withBucketAssigner(new EventTimeBucketAssigner())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(new OutputFileConfig("yiche", ".log"))
                .build();*/
        DefaultRollingPolicy po = DefaultRollingPolicy.builder()
                .withRolloverInterval(TimeUnit.MINUTES.toMillis(2))
                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                .withMaxPartSize(1024 * 1024 * 1024)
                .build();
        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(
                new Path(outputPath),
                new SimpleStringEncoder<String>())
                .withBucketAssigner(new EventTimeBucketAssigner())
                .withRollingPolicy(po)
                .withOutputFileConfig(new OutputFileConfig("yiche", ".log"))
                .build();

        // generate data, shuffle, sink
        env.addSource(new HdfsSourceTest())
                .addSink(sink);
        env.execute("test my bucket assign");
    }
}
