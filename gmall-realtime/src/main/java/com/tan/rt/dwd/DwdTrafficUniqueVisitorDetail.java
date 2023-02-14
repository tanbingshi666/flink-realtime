package com.tan.rt.dwd;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.tan.rt.utils.DateFormatUtil;
import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DwdTrafficUniqueVisitorDetail {

    private static final Logger LOG = LoggerFactory.getLogger(DwdTrafficUniqueVisitorDetail.class);

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String TOPIC = "dwd_traffic_page_log";
    private static final String GROUP_ID = "UNIQUE-VISITOR-DETAIL-0121";

    private static final String sink_topic = "dwd_traffic_unique_visitor_detail";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromSource(KafkaUtil.getKafkaSource(
                        BROKERS,
                        TOPIC,
                        GROUP_ID,
                        null
                ), WatermarkStrategy.noWatermarks(), "source kafka")
                .flatMap((FlatMapFunction<String, JSONObject>) (line, out) -> {
                    JSONObject json = (JSONObject) JSONUtil.parse(line);
                    String lastPageId = json.getByPath("page.last_page_id", String.class);

                    if (lastPageId == null) {
                        out.collect(json);
                    }
                }).returns(JSONObject.class)
                .keyBy((KeySelector<JSONObject, String>) line -> line.getByPath("common.mid", String.class))
                .filter(new RichFilterFunction<JSONObject>() {

                    private ValueState<String> lastVisitState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);
                        StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();
                        stateDescriptor.enableTimeToLive(ttlConfig);

                        lastVisitState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public boolean filter(JSONObject line) throws Exception {
                        String lastDate = lastVisitState.value();
                        Long ts = line.getLong("ts");
                        String curDate = DateFormatUtil.toDate(ts);

                        if (lastDate == null || !lastDate.equals(curDate)) {
                            lastVisitState.update(curDate);
                            return true;
                        }

                        return false;
                    }

                })
                .map(line -> line.toJSONString(0))
                .sinkTo(KafkaUtil.getKafkaSink(
                        BROKERS,
                        sink_topic,
                        null
                ));

        try {
            env.execute("DwdTrafficUniqueVisitorDetail Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
