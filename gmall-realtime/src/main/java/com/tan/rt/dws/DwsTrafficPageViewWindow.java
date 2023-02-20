package com.tan.rt.dws;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.tan.rt.bean.TrafficHomeDetailPageViewBean;
import com.tan.rt.utils.ClickHouseUtil;
import com.tan.rt.utils.DateFormatUtil;
import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficPageViewWindow {

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "dwd_traffic_page_log";
    private static final String GROUP_ID = "dws_traffic_page_view_window_0121";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromSource(
                        KafkaUtil.getKafkaSource(
                                BROKERS,
                                SOURCE_TOPIC,
                                GROUP_ID,
                                null), WatermarkStrategy.noWatermarks(),
                        "Kafka Source")
                .flatMap((FlatMapFunction<String, JSONObject>)
                        (line, out) -> {
                            JSONObject json = (JSONObject) JSONUtil.parse(line);
                            String pageId = json.getJSONObject("page").getStr("page_id");
                            if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                                out.collect(json);
                            }
                        }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(
                                Duration.ofSeconds(2)).withTimestampAssigner(
                                (SerializableTimestampAssigner<JSONObject>)
                                        (element, recordTimestamp) ->
                                                element.getLong("ts")))
                .keyBy(json -> json.getJSONObject("common").getStr("mid"))
                .flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {

                    private ValueState<String> homeLastState;
                    private ValueState<String> detailLastState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();

                        ValueStateDescriptor<String> homeStateDes = new ValueStateDescriptor<>("home-state", String.class);
                        ValueStateDescriptor<String> detailStateDes = new ValueStateDescriptor<>("detail-state", String.class);

                        homeStateDes.enableTimeToLive(ttlConfig);
                        detailStateDes.enableTimeToLive(ttlConfig);

                        homeLastState = getRuntimeContext().getState(homeStateDes);
                        detailLastState = getRuntimeContext().getState(detailStateDes);
                    }

                    @Override
                    public void flatMap(JSONObject line, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

                        Long ts = line.getLong("ts");
                        String curDt = DateFormatUtil.toDate(ts);
                        String homeLastDt = homeLastState.value();
                        String detailLastDt = detailLastState.value();

                        long homeCt = 0L;
                        long detailCt = 0L;

                        if ("home".equals(line.getJSONObject("page").getStr("page_id"))) {
                            if (homeLastDt == null || !homeLastDt.equals(curDt)) {
                                homeCt = 1L;
                                homeLastState.update(curDt);
                            }
                        } else {
                            if (detailLastDt == null || !detailLastDt.equals(curDt)) {
                                detailCt = 1L;
                                detailLastState.update(curDt);
                            }
                        }

                        if (homeCt == 1L || detailCt == 1L) {
                            out.collect(new TrafficHomeDetailPageViewBean(null, null,
                                    homeCt,
                                    detailCt,
                                    ts));
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce((ReduceFunction<TrafficHomeDetailPageViewBean>) (value1, value2) -> {
                    value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                    value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                    return value1;
                }, (AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>)
                        (window, values, out) -> {
                            TrafficHomeDetailPageViewBean pageViewBean = values.iterator().next();
                            pageViewBean.setTs(System.currentTimeMillis());
                            pageViewBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                            pageViewBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                            out.collect(pageViewBean);
                        })
                .addSink(ClickHouseUtil.getSinkFunction("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));

        try {
            env.execute("DwsTrafficPageViewWindow Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
