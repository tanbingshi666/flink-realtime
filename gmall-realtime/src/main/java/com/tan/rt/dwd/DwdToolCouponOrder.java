package com.tan.rt.dwd;

import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdToolCouponOrder {

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "topic_db";
    private static final String GROUP_ID = "TOOL-COUPON-ORDER-0121";

    private static final String SINK_TOPIC = "dwd_tool_coupon_order";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table `topic_db` ( " +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`old` map<string, string>, " +
                "`ts` string " +
                ")" + KafkaUtil.getKafkaSourceDDL(BROKERS, SOURCE_TOPIC, GROUP_ID));

        Table couponOrder = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['coupon_id'] coupon_id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "date_format(data['using_time'],'yyyy-MM-dd') date_id, " +
                "data['using_time'] using_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'update' " +
                "and data['coupon_status'] = '1402' " +
                "and `old`['coupon_status'] = '1401'");
        tableEnv.createTemporaryView("coupon_order", couponOrder);

        /**
         tableEnv.sqlQuery("select * from coupon_order")
         .execute()
         .print();
         */

        tableEnv.executeSql("create table dwd_tool_coupon_order( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "order_id string, " +
                "date_id string, " +
                "order_time string, " +
                "ts string " +
                ")" + KafkaUtil.getKafkaSinkDDL(BROKERS, SINK_TOPIC));

        tableEnv.executeSql(
                "insert into dwd_tool_coupon_order select " +
                        "id, " +
                        "coupon_id, " +
                        "user_id, " +
                        "order_id, " +
                        "date_id, " +
                        "using_time order_time, " +
                        "ts from coupon_order");

    }

}
