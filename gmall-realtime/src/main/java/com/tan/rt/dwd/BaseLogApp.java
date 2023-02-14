package com.tan.rt.dwd;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.tan.rt.utils.DateFormatUtil;
import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseLogApp {

    private static final Logger LOG = LoggerFactory.getLogger(BaseLogApp.class);

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String TOPIC = "topic_log";
    private static final String GROUP_ID = "BASE-LOG-0121";

    private static final String page_topic = "dwd_traffic_page_log";
    private static final String error_topic = "dwd_traffic_error_log";
    private static final String start_topic = "dwd_traffic_start_log";
    private static final String display_topic = "dwd_traffic_display_log";
    private static final String action_topic = "dwd_traffic_action_log";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OutputTag<String> dirtyTag = new OutputTag<String>("dirty") {
        };
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("action") {
        };
        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };

        SingleOutputStreamOperator<JSONObject> kafkaJsonDS = env.fromSource(KafkaUtil.getKafkaSource(
                                BROKERS,
                                TOPIC,
                                GROUP_ID,
                                null
                        ), WatermarkStrategy.noWatermarks(),
                        "kafka source")
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String line,
                                               ProcessFunction<String, JSONObject>.Context context,
                                               Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject json = (JSONObject) JSONUtil.parse(line);
                            out.collect(json);
                        } catch (Exception e) {
                            context.output(dirtyTag, line);
                        }
                    }
                });

        SideOutputDataStream<String> dirtyJsonDS = kafkaJsonDS.getSideOutput(dirtyTag);
        dirtyJsonDS.print("--->>>>");

        SingleOutputStreamOperator<String> pageDS = kafkaJsonDS.keyBy((KeySelector<JSONObject, String>) json -> json.getByPath("common.mid", String.class))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> lastVisitState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-visit", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject line) throws Exception {

                        String isNew = line.getByPath("common.is_new", String.class);
                        Long ts = line.getLong("ts");
                        String date = DateFormatUtil.toDate(ts);

                        String lastDate = lastVisitState.value();

                        if ("1".equals(isNew)) {
                            if (lastDate == null) {
                                lastVisitState.update(date);
                            } else if (!lastDate.equals(date)) {
                                line.putByPath("common.is_new", "0");
                            }
                        } else if (lastDate == null) {
                            lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                        }

                        return line;
                    }
                })
                .process(new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject line,
                                               ProcessFunction<JSONObject, String>.Context context,
                                               Collector<String> out) throws Exception {

                        String err = line.getStr("err");
                        if (err != null) {
                            context.output(errorTag, line.toJSONString(0));
                        }

                        line.remove("err");

                        String start = line.getStr("start");
                        if (start != null) {
                            context.output(startTag, line.toJSONString(0));
                        } else {
                            String common = line.getStr("common");
                            String pageId = line.getByPath("page.page_id", String.class);
                            Long ts = line.getLong("ts");

                            JSONArray displays = line.getJSONArray("displays");
                            if (displays != null && displays.size() > 0) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = displays.getJSONObject(i);
                                    display.set("common", common);
                                    display.set("page_id", pageId);
                                    display.set("ts", ts);
                                    context.output(displayTag, display.toJSONString(0));
                                }
                            }

                            JSONArray actions = line.getJSONArray("actions");
                            if (actions != null && actions.size() > 0) {
                                for (int i = 0; i < actions.size(); i++) {
                                    JSONObject action = actions.getJSONObject(i);
                                    action.set("common", common);
                                    action.set("page_id", pageId);
                                    context.output(actionTag, action.toJSONString(0));
                                }
                            }

                            line.remove("displays");
                            line.remove("actions");
                            out.collect(line.toJSONString(0));
                        }
                    }
                }, StringDataTypeInfo.of(String.class));

        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        pageDS.sinkTo(KafkaUtil.getKafkaSink(BROKERS, page_topic, null));
        errorDS.sinkTo(KafkaUtil.getKafkaSink(BROKERS, error_topic, null));
        startDS.sinkTo(KafkaUtil.getKafkaSink(BROKERS, start_topic, null));
        displayDS.sinkTo(KafkaUtil.getKafkaSink(BROKERS, display_topic, null));
        actionDS.sinkTo(KafkaUtil.getKafkaSink(BROKERS, action_topic, null));

        try {
            env.execute("BaseLogApp Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
