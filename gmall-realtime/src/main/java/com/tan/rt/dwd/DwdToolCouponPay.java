package com.tan.rt.dwd;

import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdToolCouponPay {

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "topic_db";
    private static final String GROUP_ID = "TOOL-COUPON-PAY-0121";

    private static final String SINK_TOPIC = "dwd_tool_coupon_pay";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table `topic_db` ( " +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`old` string, " +
                "`ts` string " +
                ")" + KafkaUtil.getKafkaSourceDDL(BROKERS, SOURCE_TOPIC, GROUP_ID));

        Table couponPay = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['coupon_id'] coupon_id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "date_format(data['used_time'],'yyyy-MM-dd') date_id, " +
                "data['used_time'] used_time, " +
                "`old`, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'update' " +
                "and data['used_time'] is not null");
        tableEnv.createTemporaryView("coupon_pay", couponPay);

        /**
         tableEnv.sqlQuery("select * from coupon_pay")
         .execute()
         .print();
         */

        tableEnv.executeSql("create table dwd_tool_coupon_pay( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "order_id string, " +
                "date_id string, " +
                "payment_time string, " +
                "ts string " +
                ")" + KafkaUtil.getKafkaSinkDDL(BROKERS, SINK_TOPIC));

        tableEnv.executeSql("insert into dwd_tool_coupon_pay select " +
                "id, " +
                "coupon_id, " +
                "user_id, " +
                "order_id, " +
                "date_id, " +
                "used_time payment_time, " +
                "ts from coupon_pay");

    }

}
