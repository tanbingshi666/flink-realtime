package com.tan.rt.dws;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.tan.rt.bean.UserLoginBean;
import com.tan.rt.utils.ClickHouseUtil;
import com.tan.rt.utils.DateFormatUtil;
import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserUserLoginWindow {

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "dwd_traffic_page_log";
    private static final String GROUP_ID = "dws_user_login_window_0121";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromSource(KafkaUtil.getKafkaSource(
                                BROKERS,
                                SOURCE_TOPIC,
                                GROUP_ID,
                                null),
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source")
                .flatMap((FlatMapFunction<String, JSONObject>)
                        (line, out) -> {
                            JSONObject json = (JSONObject) JSONUtil.parse(line);
                            String uid = json.getJSONObject("common").getStr("uid");
                            String lastPageId = json.getJSONObject("page").getStr("last_page_id");
                            if (uid != null && (lastPageId == null || lastPageId.equals("login"))) {
                                out.collect(json);
                            }
                        })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<JSONObject>)
                                                (element, recordTimestamp) ->
                                                        element.getLong("ts")))
                .keyBy(json ->
                        json.getJSONObject("common")
                                .getStr("uid"))
                .flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {

                    private ValueState<String> lastLoginState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastLoginState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-login", String.class));
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<UserLoginBean> out) throws Exception {

                        String lastLoginDt = lastLoginState.value();
                        Long ts = value.getLong("ts");
                        String curDt = DateFormatUtil.toDate(ts);

                        long uv = 0L;
                        long backUv = 0L;

                        if (lastLoginDt == null) {
                            uv = 1L;
                            lastLoginState.update(curDt);
                        } else if (!lastLoginDt.equals(curDt)) {

                            uv = 1L;
                            lastLoginState.update(curDt);

                            if ((DateFormatUtil.toTs(curDt) - DateFormatUtil.toTs(lastLoginDt)) / (24 * 60 * 60 * 1000L) >= 8) {
                                backUv = 1L;
                            }
                        }

                        if (uv != 0L) {
                            out.collect(new UserLoginBean("", "",
                                    backUv, uv, ts));
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce((ReduceFunction<UserLoginBean>) (value1, value2) -> {
                    value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                    value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                    return value1;
                }, (AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>) (window, values, out) -> {
                    UserLoginBean next = values.iterator().next();

                    next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                    next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                    next.setTs(System.currentTimeMillis());

                    out.collect(next);
                })
                .addSink(ClickHouseUtil.getSinkFunction("insert into dws_user_user_login_window values(?,?,?,?,?)"));

        try {
            env.execute("DwsUserUserLoginWindow Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
