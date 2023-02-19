package com.tan.rt.dwd;

import com.tan.rt.utils.KafkaUtil;
import com.tan.rt.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionComment {

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "topic_db";
    private static final String GROUP_ID = "INTERACTION-COMMENT-0121";

    private static final String MYSQL_HOSTNAME = "hadoop102";
    private static final Integer MySQL_PORT = 3306;
    private static final String MYSQL_DATABASE = "gmall";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "128505tan";

    private static final String SINK_TOPIC = "dwd_interaction_comment";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "5 s");

        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`proc_time` as PROCTIME(), " +
                "`ts` string " +
                ")" + KafkaUtil.getKafkaSourceDDL(BROKERS, SOURCE_TOPIC, GROUP_ID));

        Table commentInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['sku_id'] sku_id, " +
                "data['order_id'] order_id, " +
                "data['create_time'] create_time, " +
                "data['appraise'] appraise, " +
                "proc_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'comment_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("comment_info", commentInfo);

        /**
         tableEnv.sqlQuery("select * from comment_info")
         .execute()
         .print();
         */

        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL(
                MYSQL_HOSTNAME,
                MySQL_PORT,
                MYSQL_DATABASE,
                MYSQL_USERNAME,
                MYSQL_PASSWORD
        ));

        Table resultTable = tableEnv.sqlQuery("select " +
                "ci.id, " +
                "ci.user_id, " +
                "ci.sku_id, " +
                "ci.order_id, " +
                "date_format(ci.create_time,'yyyy-MM-dd') date_id, " +
                "ci.create_time, " +
                "ci.appraise, " +
                "dic.dic_name, " +
                "ts " +
                "from comment_info ci " +
                "join " +
                "base_dic for system_time as of ci.proc_time as dic " +
                "on ci.appraise = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        /**
         tableEnv.sqlQuery("select * from result_table")
         .execute()
         .print();
         */

        tableEnv.executeSql("create table dwd_interaction_comment( " +
                "id string, " +
                "user_id string, " +
                "sku_id string, " +
                "order_id string, " +
                "date_id string, " +
                "create_time string, " +
                "appraise_code string, " +
                "appraise_name string, " +
                "ts string " +
                ")" + KafkaUtil.getKafkaSinkDDL(BROKERS, SINK_TOPIC));
        tableEnv.executeSql("insert into dwd_interaction_comment select * from result_table");

    }
    
}
