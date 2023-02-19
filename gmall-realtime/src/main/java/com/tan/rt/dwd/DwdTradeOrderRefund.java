package com.tan.rt.dwd;

import com.tan.rt.utils.KafkaUtil;
import com.tan.rt.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DwdTradeOrderRefund {

    private static final Logger LOG = LoggerFactory.getLogger(DwdTradeOrderRefund.class);

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "topic_db";
    private static final String GROUP_ID = "TRADE-ORDER-REFUND-0121";

    private static final String MYSQL_HOSTNAME = "hadoop102";
    private static final Integer MySQL_PORT = 3306;
    private static final String MYSQL_DATABASE = "gmall";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "128505tan";

    private static final String SINK_TOPIC = "dwd_trade_order_refund";

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
                "`old` map<string, string>, " +
                "`proc_time` as PROCTIME(), " +
                "`ts` string " +
                ")" + KafkaUtil.getKafkaSourceDDL(BROKERS, SOURCE_TOPIC, GROUP_ID));

        Table orderRefundInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['sku_id'] sku_id, " +
                "data['refund_type'] refund_type, " +
                "data['refund_num'] refund_num, " +
                "data['refund_amount'] refund_amount, " +
                "data['refund_reason_type'] refund_reason_type, " +
                "data['refund_reason_txt'] refund_reason_txt, " +
                "data['create_time'] create_time, " +
                "proc_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'order_refund_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        /**
         tableEnv.sqlQuery("select * from order_refund_info")
         .execute()
         .print();
         */

        Table orderInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['province_id'] province_id, " +
                "`old` " +
                "from topic_db " +
                "where `table` = 'order_info' " +
                "and `type` = 'update' " +
                "and data['order_status']='1005' " +
                "and `old`['order_status'] is not null");
        tableEnv.createTemporaryView("order_info", orderInfo);

        /**
         tableEnv.sqlQuery("select * from order_info")
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

        Table refundOrder = tableEnv.sqlQuery("select  " +
                "ri.id, " +
                "ri.user_id, " +
                "ri.order_id, " +
                "ri.sku_id, " +
                "oi.province_id, " +
                "date_format(ri.create_time,'yyyy-MM-dd') date_id, " +
                "ri.create_time, " +
                "ri.refund_type, " +
                "type_dic.dic_name, " +
                "ri.refund_reason_type, " +
                "reason_dic.dic_name, " +
                "ri.refund_reason_txt, " +
                "ri.refund_num, " +
                "ri.refund_amount, " +
                "ri.ts, " +
                "current_row_timestamp() row_op_ts " +
                "from order_refund_info ri " +
                "join  " +
                "order_info oi " +
                "on ri.order_id = oi.id " +
                "join  " +
                "base_dic for system_time as of ri.proc_time as type_dic " +
                "on ri.refund_type = type_dic.dic_code " +
                "join " +
                "base_dic for system_time as of ri.proc_time as reason_dic " +
                "on ri.refund_reason_type=reason_dic.dic_code");
        tableEnv.createTemporaryView("refund_order", refundOrder);

        /**
         tableEnv.sqlQuery("select * from refund_order")
         .execute()
         .print();
         */

        tableEnv.executeSql("create table dwd_trade_order_refund( " +
                "id string, " +
                "user_id string, " +
                "order_id string, " +
                "sku_id string, " +
                "province_id string, " +
                "date_id string, " +
                "create_time string, " +
                "refund_type_code string, " +
                "refund_type_name string, " +
                "refund_reason_type_code string, " +
                "refund_reason_type_name string, " +
                "refund_reason_txt string, " +
                "refund_num string, " +
                "refund_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + KafkaUtil.getKafkaSinkDDL(BROKERS, SINK_TOPIC));

        tableEnv.executeSql("insert into dwd_trade_order_refund select * from refund_order");

    }

}
