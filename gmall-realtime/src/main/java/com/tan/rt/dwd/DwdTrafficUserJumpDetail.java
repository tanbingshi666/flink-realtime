package com.tan.rt.dwd;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class DwdTrafficUserJumpDetail {

    private static final Logger LOG = LoggerFactory.getLogger(DwdTrafficUserJumpDetail.class);

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String TOPIC = "dwd_traffic_page_log";
    private static final String GROUP_ID = "USER-JUMP-DETAIL-0121";

    private static final String sink_topic = "dwd_traffic_user_jump_detail";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // {"common":{"ar":"110000","ba":"Xiaomi","ch":"vivo","is_new":"1","md":"Xiaomi 10 Pro ","mid":"mid_968529","os":"Android 11.0","uid":"722","vc":"v2.1.132"},"page":{"during_time":5500,"item":"小米盒子","item_type":"keyword","last_page_id":"search","page_id":"good_list"},"ts":1674280082000}
        KeyedStream<JSONObject, String> kafkaJsonDS = env.fromSource(KafkaUtil.getKafkaSource(
                        BROKERS,
                        TOPIC,
                        GROUP_ID,
                        null
                ), WatermarkStrategy.noWatermarks(), "source kafka")
                .map(line -> (JSONObject) JSONUtil.parse(line)).returns(JSONObject.class)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (line, systemTs) -> line.getLong("ts"))
                ).keyBy((KeySelector<JSONObject, String>) line -> line.getByPath("common.mid", String.class));

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject line) throws Exception {
                return line.getJSONObject("page").getStr("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject line) throws Exception {
                return line.getJSONObject("page").getStr("last_page_id") == null;
            }
        }).within(Time.seconds(10));

        OutputTag<String> timeoutTag = new OutputTag<String>("timeOut") {
        };

        SingleOutputStreamOperator<String> patternDS = CEP.pattern(kafkaJsonDS, pattern)
                .select(
                        timeoutTag,
                        (PatternTimeoutFunction<JSONObject, String>) (map, systemTs) -> map.get("start").get(0).toJSONString(0),
                        (PatternSelectFunction<JSONObject, String>) map -> map.get("start").get(0).toJSONString(0));

        SideOutputDataStream<String> timeoutDS = patternDS.getSideOutput(timeoutTag);
        patternDS.union(timeoutDS)
                .sinkTo(KafkaUtil.getKafkaSink(
                        BROKERS,
                        sink_topic,
                        null
                ));

        try {
            env.execute("DwdTrafficUserJumpDetail Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
