package com.tan.rt.dws;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.tan.rt.bean.TradePaymentWindowBean;
import com.tan.rt.utils.ClickHouseUtil;
import com.tan.rt.utils.DateFormatUtil;
import com.tan.rt.utils.KafkaUtil;
import com.tan.rt.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradePaymentSucWindow {

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "dwd_trade_pay_detail_suc";
    private static final String GROUP_ID = "dws_trade_payment_suc_window_0121";

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
                .flatMap((FlatMapFunction<String, JSONObject>) (line, out) -> {
                    try {
                        JSONObject json = (JSONObject) JSONUtil.parse(line);
                        out.collect(json);
                    } catch (Exception ignored) {
                    }
                })
                .keyBy(json -> json.getStr("order_detail_id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("value-state", JSONObject.class));
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

                        JSONObject state = valueState.value();

                        if (state == null) {
                            valueState.update(value);
                            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
                        } else {
                            String stateRt = state.getStr("row_op_ts");
                            String curRt = value.getStr("row_op_ts");

                            int compare = TimestampLtz3CompareUtil.compare(stateRt, curRt);

                            if (compare != 1) {
                                valueState.update(value);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        JSONObject value = valueState.value();
                        out.collect(value);
                        valueState.clear();
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject line, long recordTimestamp) {
                        String callbackTime = line.getStr("callback_time");
                        return DateFormatUtil.toTs(callbackTime, true);
                    }
                }))
                .keyBy(json -> json.getStr("user_id"))
                .flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {

                    private ValueState<String> lastDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-dt", String.class));
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<TradePaymentWindowBean> out) throws Exception {

                        String lastDt = lastDtState.value();
                        String curDt = value.getStr("callback_time").split(" ")[0];

                        long paymentSucUniqueUserCount = 0L;
                        long paymentSucNewUserCount = 0L;

                        if (lastDt == null) {
                            paymentSucUniqueUserCount = 1L;
                            paymentSucNewUserCount = 1L;
                            lastDtState.update(curDt);
                        } else if (!lastDt.equals(curDt)) {
                            paymentSucUniqueUserCount = 1L;
                            lastDtState.update(curDt);
                        }

                        if (paymentSucUniqueUserCount == 1L) {
                            out.collect(new TradePaymentWindowBean(null, null,
                                    paymentSucUniqueUserCount,
                                    paymentSucNewUserCount,
                                    null));
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce((ReduceFunction<TradePaymentWindowBean>)
                        (value1, value2) -> {
                            value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                            value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                            return value1;
                        }, (AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>)
                        (window, values, out) -> {
                            TradePaymentWindowBean next = values.iterator().next();
                            next.setTs(System.currentTimeMillis());
                            next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                            next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                            out.collect(next);
                        })
                .addSink(ClickHouseUtil.getSinkFunction("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));

        try {
            env.execute("DwsTradePaymentSucWindow Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
