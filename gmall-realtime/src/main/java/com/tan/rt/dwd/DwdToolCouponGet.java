package com.tan.rt.dwd;

import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdToolCouponGet {

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "topic_db";
    private static final String GROUP_ID = "TOOL-COUPON-GET-0121";

    private static final String SINK_TOPIC = "dwd_tool_coupon_get";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table `topic_db`( " +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`ts` string " +
                ")" + KafkaUtil.getKafkaSourceDDL(BROKERS, SOURCE_TOPIC, GROUP_ID));

        Table couponGet = tableEnv.sqlQuery("select " +
                "data['id'], " +
                "data['coupon_id'], " +
                "data['user_id'], " +
                "date_format(data['get_time'],'yyyy-MM-dd') date_id, " +
                "data['get_time'], " +
                "ts " +
                "from topic_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("coupon_get", couponGet);

        /**
         tableEnv.sqlQuery("select * from coupon_get")
         .execute()
         .print();
         */

        tableEnv.executeSql("create table dwd_tool_coupon_get ( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "date_id string, " +
                "get_time string, " +
                "ts string " +
                ")" + KafkaUtil.getKafkaSinkDDL(BROKERS, SINK_TOPIC));

        tableEnv.executeSql("insert into dwd_tool_coupon_get select * from coupon_get");


    }

}
