package com.tan.rt.dwd;

import com.tan.rt.utils.KafkaUtil;
import com.tan.rt.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DwdTradeRefundPaySuc {

    private static final Logger LOG = LoggerFactory.getLogger(DwdTradeRefundPaySuc.class);

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "topic_db";
    private static final String GROUP_ID = "TRADE-REFUND-PAY-SUC-0121";

    private static final String MYSQL_HOSTNAME = "hadoop102";
    private static final Integer MySQL_PORT = 3306;
    private static final String MYSQL_DATABASE = "gmall";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "128505tan";

    private static final String SINK_TOPIC = "dwd_trade_refund_pay_suc";

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

        Table refundPayment = tableEnv.sqlQuery("select " +
                        "data['id'] id, " +
                        "data['order_id'] order_id, " +
                        "data['sku_id'] sku_id, " +
                        "data['payment_type'] payment_type, " +
                        "data['callback_time'] callback_time, " +
                        "data['total_amount'] total_amount, " +
                        "proc_time, " +
                        "ts " +
                        "from topic_db " +
                        "where `table` = 'refund_payment' "
//                +
//                "and `type` = 'update' " +
//                "and data['refund_status'] = '0702' " +
//                "and `old`['refund_status'] is not null"
        );
        tableEnv.createTemporaryView("refund_payment", refundPayment);

        /**
         tableEnv.sqlQuery("select * from refund_payment")
         .execute()
         .print();
         */

        Table orderInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['province_id'] province_id, " +
                "`old` " +
                "from topic_db " +
                "where `table` = 'order_info' " +
                "and `type` = 'update' "
                +
                "and data['order_status']='1006' " +
                "and `old`['order_status'] is not null"
        );
        tableEnv.createTemporaryView("order_info", orderInfo);

        /**
         tableEnv.sqlQuery("select * from order_info")
         .execute()
         .print();
         */

        Table orderRefundInfo = tableEnv.sqlQuery("select " +
                        "data['order_id'] order_id, " +
                        "data['sku_id'] sku_id, " +
                        "data['refund_num'] refund_num, " +
                        "`old` " +
                        "from topic_db " +
                        "where `table` = 'order_refund_info' "
//                        +
//                        "and `type` = 'update' " +
//                        "and data['refund_status']='0705' " +
//                        "and `old`['refund_status'] is not null"
        );
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        /**
         tableEnv.sqlQuery("select * from order_refund_info")
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

        Table refundPaySuc = tableEnv.sqlQuery("select " +
                "rp.id, " +
                "oi.user_id, " +
                "rp.order_id, " +
                "rp.sku_id, " +
                "oi.province_id, " +
                "rp.payment_type, " +
                "dic.dic_name payment_type_name, " +
                "date_format(rp.callback_time,'yyyy-MM-dd') date_id, " +
                "rp.callback_time, " +
                "ri.refund_num, " +
                "rp.total_amount, " +
                "rp.ts, " +
                "current_row_timestamp() row_op_ts " +
                "from refund_payment rp  " +
                "join  " +
                "order_info oi " +
                "on rp.order_id = oi.id " +
                "join " +
                "order_refund_info ri " +
                "on rp.order_id = ri.order_id " +
                "and rp.sku_id = ri.sku_id " +
                "join  " +
                "base_dic for system_time as of rp.proc_time as dic " +
                "on rp.payment_type = dic.dic_code ");
        tableEnv.createTemporaryView("refund_pay_suc", refundPaySuc);

        /**
         tableEnv.sqlQuery("select * from refund_pay_suc")
         .execute()
         .print();
         */

        tableEnv.executeSql("create table dwd_trade_refund_pay_suc( " +
                "id string, " +
                "user_id string, " +
                "order_id string, " +
                "sku_id string, " +
                "province_id string, " +
                "payment_type_code string, " +
                "payment_type_name string, " +
                "date_id string, " +
                "callback_time string, " +
                "refund_num string, " +
                "refund_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + KafkaUtil.getKafkaSinkDDL(BROKERS, SINK_TOPIC));
        tableEnv.executeSql("insert into dwd_trade_refund_pay_suc select * from refund_pay_suc");

    }

}
