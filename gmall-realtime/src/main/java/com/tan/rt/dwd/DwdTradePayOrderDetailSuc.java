package com.tan.rt.dwd;

import com.tan.rt.utils.KafkaUtil;
import com.tan.rt.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class DwdTradePayOrderDetailSuc {

    private static final Logger LOG = LoggerFactory.getLogger(DwdTradePayOrderDetailSuc.class);

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC_ORDER_DETAIL = "dwd_trade_order_detail";
    private static final String GROUP_ID_PAY = "TRADE-PAY-ORDER-DETAIL-0121";
    private static final String GROUP_ID_ORDER = "TRADE-ORDER-DETAIL-0121";

    private static final String MYSQL_HOSTNAME = "hadoop102";
    private static final Integer MySQL_PORT = 3306;
    private static final String MYSQL_DATABASE = "gmall";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "128505tan";

    private static final String SINK_TOPIC = "dwd_trade_pay_order_detail_suc";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15L * 60L + 5L));

        tableEnv.executeSql(
                KafkaUtil.getTopicDb(
                        BROKERS,
                        GROUP_ID_PAY));

        Table paymentInfo = tableEnv.sqlQuery("select " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['payment_type'] payment_type, " +
                "data['callback_time'] callback_time, " +
                "`pt` " +
                "from topic_db " +
                "where `table` = 'payment_info' " +
                "and `type` = 'update' " +
                "and data['payment_status']='1602'");
        tableEnv.createTemporaryView("payment_info", paymentInfo);

        /**
         tableEnv.sqlQuery("select * from payment_info")
         .execute()
         .print();
         */

        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "sku_num string, " +
                "order_price string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "create_time string, " +
                "source_id string, " +
                "source_type_id string, " +
                "source_type_name string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string, " +
                "row_op_ts timestamp_ltz(3) " +
                ") " +
                KafkaUtil.getKafkaSourceDDL(
                        BROKERS,
                        SOURCE_TOPIC_ORDER_DETAIL,
                        GROUP_ID_ORDER));

        /**
         tableEnv.sqlQuery("select * from dwd_trade_order_detail")
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

        Table payOrderDetailSuc = tableEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id, " +
                        "od.order_id, " +
                        "od.user_id, " +
                        "od.sku_id, " +
                        "od.sku_name, " +
                        "od.province_id, " +
                        "od.activity_id, " +
                        "od.activity_rule_id, " +
                        "od.coupon_id, " +
                        "pi.payment_type payment_type_code, " +
                        "dic.dic_name payment_type_name, " +
                        "pi.callback_time, " +
                        "od.source_id, " +
                        "od.source_type_id, " +
                        "od.source_type_name, " +
                        "od.sku_num, " +
                        "od.order_price, " +
                        "od.split_activity_amount, " +
                        "od.split_coupon_amount, " +
                        "od.split_total_amount split_payment_amount, " +
                        "od.row_op_ts row_op_ts " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id = od.order_id " +
                        "join `base_dic` for system_time as of pi.pt as dic " +
                        "on pi.payment_type = dic.dic_code");
        tableEnv.createTemporaryView("pay_order_detail_suc", payOrderDetailSuc);

        /**
         tableEnv.sqlQuery("select * from pay_order_detail_suc")
         .execute()
         .print();
         */

        tableEnv.executeSql(
                "create table dwd_trade_pay_order_detail_suc( " +
                        "order_detail_id string, " +
                        "order_id string, " +
                        "user_id string, " +
                        "sku_id string, " +
                        "sku_name string, " +
                        "province_id string, " +
                        "activity_id string, " +
                        "activity_rule_id string, " +
                        "coupon_id string, " +
                        "payment_type_code string, " +
                        "payment_type_name string, " +
                        "callback_time string, " +
                        "source_id string, " +
                        "source_type_id string, " +
                        "source_type_name string, " +
                        "sku_num string, " +
                        "order_price string, " +
                        "split_activity_amount string, " +
                        "split_coupon_amount string, " +
                        "split_payment_amount string, " +
                        "row_op_ts timestamp_ltz(3), " +
                        "primary key(order_detail_id) not enforced " +
                        ") " +
                        KafkaUtil.getKafkaUpsertDDL(
                                BROKERS,
                                SINK_TOPIC));

        tableEnv.executeSql("insert into dwd_trade_pay_order_detail_suc select * from pay_order_detail_suc");

    }

}
