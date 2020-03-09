package com.atguigu.apitest.sinkTest.hdfs;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.scala.DataStream;

import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

public class StreamSink {
    public static StreamingFileSink<String> getHdfsSink(String path, BucketAssigner<String, String> bucketAssigner, RollingPolicy policy, String prefix, String suffix) {
        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(
                new Path(path),
                new SimpleStringEncoder<String>()
                ).withBucketAssigner(bucketAssigner)
                .withRollingPolicy(policy)
                .withBucketCheckInterval(15000)
                .withOutputFileConfig(new OutputFileConfig(prefix, suffix))
                .build();
        return sink;
    }

    public static void getHdfsSinkExe(DataStream<String> stream, String path, BucketAssigner<String, String> bucketAssigner, RollingPolicy policy, String prefix, String suffix) {
        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(
                new Path(path),
                new SimpleStringEncoder<String>()
        ).withBucketAssigner(bucketAssigner)
                .withRollingPolicy(policy)
                .withBucketCheckInterval(15000)
                .withOutputFileConfig(new OutputFileConfig(prefix, suffix))
                .build();
        stream.addSink(sink);
    }

    public static StreamingFileSink<String> oneHourRoll(String path) {
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".ext")
                .build();
        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(
                new Path(path),
                new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new EventTimeBucketAssigner())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(config)
                .build();
        return sink;
    }

    public static StreamingFileSink<String> test(String path) {
//        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(
//                new Path(path),
//                new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                /*.withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(2))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build())*/
//                .build();
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(path), (Encoder<String>) (element, stream) -> {
                    PrintStream out = new PrintStream(stream);
                    out.println(element);
                })
                .withBucketAssigner(new EventTimeBucketAssigner())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(new OutputFileConfig("yiche", ".log"))
                .build();
        return sink;
    }
}
