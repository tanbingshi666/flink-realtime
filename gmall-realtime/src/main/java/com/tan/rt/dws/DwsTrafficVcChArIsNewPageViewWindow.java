package com.tan.rt.dws;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.tan.rt.bean.TrafficPageViewBean;
import com.tan.rt.utils.ClickHouseUtil;
import com.tan.rt.utils.DateFormatUtil;
import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindow {

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC_UV = "dwd_traffic_unique_visitor_detail";
    private static final String SOURCE_TOPIC_UJD = "dwd_traffic_user_jump_detail";
    private static final String SOURCE_TOPIC_PAGE = "dwd_traffic_page_log";
    private static final String GROUP_ID_UV = "dws_traffic_vc_ch_is_new_page_view_window_uv_0121";
    private static final String GROUP_ID_UJD = "dws_traffic_vc_ch_is_new_page_view_window_ujd_0121";
    private static final String GROUP_ID_PAGE = "dws_traffic_vc_ch_is_new_page_view_window_page_0121";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> uvDS = env.fromSource(KafkaUtil.getKafkaSource(
                        BROKERS,
                        SOURCE_TOPIC_UV,
                        GROUP_ID_UV,
                        null), WatermarkStrategy.noWatermarks(),
                "kafka source Uv");
        DataStreamSource<String> ujDS = env.fromSource(KafkaUtil.getKafkaSource(
                        BROKERS,
                        SOURCE_TOPIC_UJD,
                        GROUP_ID_UJD,
                        null), WatermarkStrategy.noWatermarks(),
                "kafka source Ujd");
        DataStreamSource<String> pageDS = env.fromSource(KafkaUtil.getKafkaSource(
                        BROKERS,
                        SOURCE_TOPIC_PAGE,
                        GROUP_ID_PAGE,
                        null), WatermarkStrategy.noWatermarks(),
                "kafka source Page");

        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUvDS = uvDS.map(line -> {
            JSONObject json = (JSONObject) JSONUtil.parse(line);
            JSONObject common = json.getJSONObject("common");

            return new TrafficPageViewBean(null, null,
                    common.getStr("vc"),
                    common.getStr("ch"),
                    common.getStr("ar"),
                    common.getStr("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    json.getLong("ts"));
        });

        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUjDS = ujDS.map(line -> {
            JSONObject json = (JSONObject) JSONUtil.parse(line);
            JSONObject common = json.getJSONObject("common");

            return new TrafficPageViewBean(null, null,
                    common.getStr("vc"),
                    common.getStr("ch"),
                    common.getStr("ar"),
                    common.getStr("is_new"),
                    0L, 0L, 0L, 0L, 1L,
                    json.getLong("ts"));
        });

        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithPageDS = pageDS.map(line -> {
            JSONObject json = (JSONObject) JSONUtil.parse(line);
            JSONObject common = json.getJSONObject("common");

            JSONObject page = json.getJSONObject("page");
            String lastPageId = page.getStr("last_page_id");
            long sv = 0L;
            if (lastPageId == null) {
                sv = 1L;
            }

            return new TrafficPageViewBean("", "",
                    common.getStr("vc"),
                    common.getStr("ch"),
                    common.getStr("ar"),
                    common.getStr("is_new"),
                    0L, sv, 1L, page.getLong("during_time"), 0L,
                    json.getLong("ts"));
        });

        SingleOutputStreamOperator<TrafficPageViewBean> resultDS = trafficPageViewWithUvDS.union(
                        trafficPageViewWithUjDS,
                        trafficPageViewWithPageDS)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(14))
                                .withTimestampAssigner((SerializableTimestampAssigner<TrafficPageViewBean>) (
                                        element, recordTimestamp) -> element.getTs()))
                .keyBy((KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>)
                        value -> new Tuple4<>(value.getAr(),
                                value.getCh(),
                                value.getIsNew(),
                                value.getVc()))
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce((ReduceFunction<TrafficPageViewBean>) (value1, value2) -> {
                    value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                    value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                    value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                    value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                    value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                    return value1;
                }, (WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>)
                        (key, window, input, out) -> {
                            TrafficPageViewBean next = input.iterator().next();

                            next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                            next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                            next.setTs(System.currentTimeMillis());

                            out.collect(next);
                        });

        resultDS.addSink(ClickHouseUtil.getSinkFunction("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        try {
            env.execute("DwsTrafficVcChArIsNewPageViewWindow Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
