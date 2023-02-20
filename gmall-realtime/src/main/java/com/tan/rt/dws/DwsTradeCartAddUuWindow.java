package com.tan.rt.dws;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.tan.rt.bean.CartAddUuBean;
import com.tan.rt.utils.ClickHouseUtil;
import com.tan.rt.utils.DateFormatUtil;
import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

public class DwsTradeCartAddUuWindow {

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "dwd_trade_cart_add";
    private static final String GROUP_ID = "dws_trade_cart_add_uu_window_0121";

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
                .map(line -> (JSONObject) JSONUtil.parse(line))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(
                                Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>)
                                (element, recordTimestamp) -> {
                                    String operateTime = element.getStr("operate_time");
                                    if (operateTime != null) {
                                        return DateFormatUtil.toTs(operateTime, true);
                                    } else {
                                        return DateFormatUtil.toTs(element.getStr("create_time"), true);
                                    }
                                }))
                .keyBy(json -> json.getStr("user_id"))
                .flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {

                    private ValueState<String> lastCartAddState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();

                        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last-cart", String.class);
                        stateDescriptor.enableTimeToLive(ttlConfig);

                        lastCartAddState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {

                        String lastDt = lastCartAddState.value();
                        String operateTime = value.getStr("operate_time");
                        String curDt = null;
                        if (operateTime != null) {
                            curDt = operateTime.split(" ")[0];
                        } else {
                            String createTime = value.getStr("create_time");
                            curDt = createTime.split(" ")[0];
                        }

                        if (lastDt == null || !lastDt.equals(curDt)) {
                            lastCartAddState.update(curDt);
                            out.collect(new CartAddUuBean(null, null,
                                    1L,
                                    null));
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce((ReduceFunction<CartAddUuBean>)
                        (value1, value2) -> {
                            value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                            return value1;
                        }, (AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>)
                        (window, values, out) -> {
                            CartAddUuBean next = values.iterator().next();

                            next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                            next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                            next.setTs(System.currentTimeMillis());

                            out.collect(next);
                        })
                .addSink(ClickHouseUtil.getSinkFunction("insert into dws_trade_cart_add_uu_window values (?,?,?,?)"));

        try {
            env.execute("DwsTradeCartAddUuWindow Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
