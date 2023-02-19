package com.tan.rt.dwd;

import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionFavorAdd {

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "topic_db";
    private static final String GROUP_ID = "INTERACTION-FAVOR-ADD-0121";

    private static final String SINK_TOPIC = "dwd_interaction_favor_add";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`ts` string " +
                ")" + KafkaUtil.getKafkaSourceDDL(BROKERS, SOURCE_TOPIC, GROUP_ID));

        Table favorInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['sku_id'] sku_id, " +
                "date_format(data['create_time'],'yyyy-MM-dd') date_id, " +
                "data['create_time'] create_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'favor_info' " +
                "and (`type` = 'insert' or (`type` = 'update' and data['is_cancel'] = '0'))" +
                "");
        tableEnv.createTemporaryView("favor_info", favorInfo);

        /**
         tableEnv.sqlQuery("select * from favor_info")
         .execute()
         .print();
         */

        tableEnv.executeSql("create table dwd_interaction_favor_add ( " +
                "id string, " +
                "user_id string, " +
                "sku_id string, " +
                "date_id string, " +
                "create_time string, " +
                "ts string " +
                ")" + KafkaUtil.getKafkaSinkDDL(BROKERS, SINK_TOPIC));

        tableEnv.executeSql("insert into dwd_interaction_favor_add select * from favor_info");

    }

}
