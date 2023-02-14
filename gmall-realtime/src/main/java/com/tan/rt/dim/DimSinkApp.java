package com.tan.rt.dim;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.tan.rt.bean.TableProcess;
import com.tan.rt.func.DimSinkFunction;
import com.tan.rt.func.TableProcessFunction;
import com.tan.rt.utils.KafkaUtil;
import com.tan.rt.utils.MysqlUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 需求：读取 Kafka topic_db 主题数据，匹配配置表过滤业务维表数据到 hbase
 */
public class DimSinkApp {

    private static final Logger LOG = LoggerFactory.getLogger(DimSinkApp.class);

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String TOPIC = "topic_db";
    private static final String GROUP_ID = "DIM-APP-0121";

    private static final String HOSTNAME = "hadoop102";
    private static final Integer PORT = 3306;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "128505tan";
    private static final String[] DATABASES = {"gmall_config"};
    private static final String[] TABLES = {"gmall_config.table_process"};

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "tanbs");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 生产环境下设置 kafka 主题分区数，测试环境下都设置为 1
        env.setParallelism(1);

        // 生产环境下开启 checkpoint & 配置 state backend
        /**
         // 开启 checkpoint
         env.enableCheckpointing(60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
         env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
         env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30 * 1000L);
         // 配置了 MinPauseBetweenCheckpoints 就不能设置 MaxConcurrentCheckpoints
         // env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
         env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

         // 配置 state backend
         env.setStateBackend(new HashMapStateBackend());
         env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://hadoop102:8020/flink-gmall/ck"))
         */

        SingleOutputStreamOperator<JSONObject> kafkaJsonDS = env.fromSource(KafkaUtil.getKafkaSource(
                                BROKERS,
                                TOPIC,
                                GROUP_ID,
                                null
                        ), WatermarkStrategy.noWatermarks(),
                        "kafka source")
                .flatMap((FlatMapFunction<String, JSONObject>) (line, out) -> {
                    try {
                        JSONObject json = (JSONObject) JSONUtil.parse(line);
                        String type = json.getStr("type");

                        if ("insert".equals(type) ||
                                "update".equals(type) ||
                                "bootstrap-insert".equals(type)) {
                            out.collect(json);
                        }
                    } catch (Exception e) {
                        LOG.error("解析 kafka {} 主题, 发现脏数据, {}", TOPIC, line);
                    }
                }).returns(JSONObject.class);

        DataStreamSource<String> mysqlJsonDS = env.fromSource(MysqlUtil.getMySqlSource(
                HOSTNAME,
                PORT,
                USERNAME,
                PASSWORD,
                DATABASES,
                TABLES,
                null
        ), WatermarkStrategy.noWatermarks(), "mysql source");

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>(
                "map-state",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<TableProcess>() {
                })
        );
        BroadcastStream<String> broadcastStream = mysqlJsonDS
                .broadcast(mapStateDescriptor);

        kafkaJsonDS.connect(broadcastStream)
                .process(new TableProcessFunction(mapStateDescriptor))
                .addSink(new DimSinkFunction());

        try {
            env.execute("DimSinkApp Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
