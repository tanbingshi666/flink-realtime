package com.tan.rt.dws;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.tan.rt.bean.UserRegisterBean;
import com.tan.rt.utils.ClickHouseUtil;
import com.tan.rt.utils.DateFormatUtil;
import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class DwsUserUserRegisterWindow {

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "dwd_user_register";
    private static final String GROUP_ID = "dws_user_user_register_window_0121";

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
                .map(line -> {
                    JSONObject json = (JSONObject) JSONUtil.parse(line);
                    String createTime = json.getStr("create_time");

                    return new UserRegisterBean(null, null,
                            1L,
                            DateFormatUtil.toTs(createTime, true));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(
                                Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<UserRegisterBean>)
                                (element, recordTimestamp) ->
                                        element.getTs()))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce((ReduceFunction<UserRegisterBean>) (value1, value2) -> {
                    value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                    return value1;
                }, (AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>) (window, values, out) -> {
                    UserRegisterBean userRegisterBean = values.iterator().next();

                    userRegisterBean.setTs(System.currentTimeMillis());
                    userRegisterBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                    userRegisterBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                    out.collect(userRegisterBean);
                }).addSink(ClickHouseUtil.getSinkFunction("insert into dws_user_user_register_window values(?,?,?,?)"));

        try {
            env.execute("DwsUserUserRegisterWindow Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
