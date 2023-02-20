package com.tan.rt.dws;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.tan.rt.bean.TradeOrderBean;
import com.tan.rt.utils.ClickHouseUtil;
import com.tan.rt.utils.DateFormatUtil;
import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class DwsTradeOrderWindow {

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "dwd_trade_order_detail";
    private static final String GROUP_ID = "dws_trade_order_window_0121";

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
                            try {
                                JSONObject jsonObject = (JSONObject) JSONUtil.parse(line);
                                out.collect(jsonObject);
                            } catch (Exception ignored) {
                            }
                        }).keyBy(json -> json.getStr("id"))
                .filter(new RichFilterFunction<JSONObject>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .build();
                        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("is-exists", String.class);
                        stateDescriptor.enableTimeToLive(ttlConfig);

                        valueState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public boolean filter(JSONObject value) throws Exception {

                        String state = valueState.value();

                        if (state == null) {
                            valueState.update("1");
                            return true;
                        } else {
                            return false;
                        }
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(
                                Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>)
                                (element, recordTimestamp) ->
                                        DateFormatUtil.toTs(element.getStr("create_time"), true)))
                .keyBy(json -> json.getStr("user_id"))
                .map(new RichMapFunction<JSONObject, TradeOrderBean>() {

                    private ValueState<String> lastOrderDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastOrderDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-order", String.class));
                    }

                    @Override
                    public TradeOrderBean map(JSONObject value) throws Exception {

                        String lastOrderDt = lastOrderDtState.value();
                        String curDt = value.getStr("create_time").split(" ")[0];

                        long orderUniqueUserCount = 0L;
                        long orderNewUserCount = 0L;

                        if (lastOrderDt == null) {
                            orderUniqueUserCount = 1L;
                            orderNewUserCount = 1L;

                            lastOrderDtState.update(curDt);
                        } else if (!lastOrderDt.equals(curDt)) {
                            orderUniqueUserCount = 1L;
                            lastOrderDtState.update(curDt);
                        }

                        Integer skuNum = value.getInt("sku_num");
                        Double orderPrice = value.getDouble("order_price");

                        Double splitActivityAmount = value.getDouble("split_activity_amount");
                        if (splitActivityAmount == null) {
                            splitActivityAmount = 0.0D;
                        }
                        Double splitCouponAmount = value.getDouble("split_coupon_amount");
                        if (splitCouponAmount == null) {
                            splitCouponAmount = 0.0D;
                        }

                        return new TradeOrderBean(null, null,
                                orderUniqueUserCount,
                                orderNewUserCount,
                                splitActivityAmount,
                                splitCouponAmount,
                                skuNum * orderPrice,
                                null);
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce((ReduceFunction<TradeOrderBean>)
                        (value1, value2) -> {
                            value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                            value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                            value1.setOrderOriginalTotalAmount(value1.getOrderOriginalTotalAmount() + value2.getOrderOriginalTotalAmount());
                            value1.setOrderActivityReduceAmount(value1.getOrderActivityReduceAmount() + value2.getOrderActivityReduceAmount());
                            value1.setOrderCouponReduceAmount(value1.getOrderCouponReduceAmount() + value2.getOrderCouponReduceAmount());
                            return value1;
                        }, (AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>)
                        (window, values, out) -> {
                            TradeOrderBean tradeOrderBean = values.iterator().next();

                            tradeOrderBean.setTs(System.currentTimeMillis());
                            tradeOrderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                            tradeOrderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                            out.collect(tradeOrderBean);
                        })
                .addSink(ClickHouseUtil.getSinkFunction("insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"));

        try {
            env.execute("DwsTradeOrderWindow Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}