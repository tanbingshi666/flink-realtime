package com.tan.rt.dwd;

import com.tan.rt.utils.KafkaUtil;
import com.tan.rt.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class DwdTradeOrderPreProcess {

    private static final Logger LOG = LoggerFactory.getLogger(DwdTradeOrderPreProcess.class);

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String GROUP_ID = "TRADE-ORDER-PRE-PROCESS-0121";

    private static final String MYSQL_HOSTNAME = "hadoop102";
    private static final Integer MySQL_PORT = 3306;
    private static final String MYSQL_DATABASE = "gmall";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "128505tan";

    private static final String SINK_TOPIC = "dwd_trade_order_pre_process";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30L));

        tableEnv.executeSql(KafkaUtil.getTopicDb(BROKERS, GROUP_ID));

        Table orderDetail = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['sku_name'] sku_name, " +
                "    data['order_price'] order_price, " +
                "    data['sku_num'] sku_num, " +
                "    data['create_time'] create_time, " +
                "    data['source_type'] source_type, " +
                "    data['source_id'] source_id, " +
                "    data['split_total_amount'] split_total_amount, " +
                "    data['split_activity_amount'] split_activity_amount, " +
                "    data['split_coupon_amount'] split_coupon_amount, " +
                "    pt  " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail", orderDetail);

        /**
         tableEnv.sqlQuery("select * from order_detail")
         .execute()
         .print();
         */

        Table orderInfo = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['consignee'] consignee, " +
                "    data['consignee_tel'] consignee_tel, " +
                "    data['total_amount'] total_amount, " +
                "    data['order_status'] order_status, " +
                "    data['user_id'] user_id, " +
                "    data['payment_way'] payment_way, " +
                "    data['delivery_address'] delivery_address, " +
                "    data['order_comment'] order_comment, " +
                "    data['out_trade_no'] out_trade_no, " +
                "    data['trade_body'] trade_body, " +
                "    data['create_time'] create_time, " +
                "    data['operate_time'] operate_time, " +
                "    data['expire_time'] expire_time, " +
                "    data['process_status'] process_status, " +
                "    data['tracking_no'] tracking_no, " +
                "    data['parent_order_id'] parent_order_id, " +
                "    data['province_id'] province_id, " +
                "    data['activity_reduce_amount'] activity_reduce_amount, " +
                "    data['coupon_reduce_amount'] coupon_reduce_amount, " +
                "    data['original_total_amount'] original_total_amount, " +
                "    data['feight_fee'] feight_fee, " +
                "    data['feight_fee_reduce'] feight_fee_reduce, " +
                "    data['refundable_time'] refundable_time, " +
                "    `type`, " +
                "    `old` " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_info' " +
                "and (`type` = 'insert' or `type` = 'update')");
        tableEnv.createTemporaryView("order_info", orderInfo);

        /**
         tableEnv.sqlQuery("select * from order_info")
         .execute()
         .print();
         */

        Table orderDetailActivity = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['order_detail_id'] order_detail_id, " +
                "    data['activity_id'] activity_id, " +
                "    data['activity_rule_id'] activity_rule_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['create_time'] create_time " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail_activity' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        /**
         tableEnv.sqlQuery("select * from order_detail_activity")
         .execute()
         .print();
         */

        Table orderDetailCoupon = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['order_detail_id'] order_detail_id, " +
                "    data['coupon_id'] coupon_id, " +
                "    data['coupon_use_id'] coupon_use_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['create_time'] create_time " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail_coupon' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        /**
         tableEnv.sqlQuery("select * from order_detail_coupon")
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

        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    od.id, " +
                "    od.order_id, " +
                "    od.sku_id, " +
                "    od.sku_name, " +
                "    od.order_price, " +
                "    od.sku_num, " +
                "    od.create_time, " +
                "    od.source_type source_type_id, " +
                "    dic.dic_name source_type_name, " +
                "    od.source_id, " +
                "    od.split_total_amount, " +
                "    od.split_activity_amount, " +
                "    od.split_coupon_amount, " +
                "    oi.consignee, " +
                "    oi.consignee_tel, " +
                "    oi.total_amount, " +
                "    oi.order_status, " +
                "    oi.user_id, " +
                "    oi.payment_way, " +
                "    oi.delivery_address, " +
                "    oi.order_comment, " +
                "    oi.out_trade_no, " +
                "    oi.trade_body, " +
                "    oi.operate_time, " +
                "    oi.expire_time, " +
                "    oi.process_status, " +
                "    oi.tracking_no, " +
                "    oi.parent_order_id, " +
                "    oi.province_id, " +
                "    oi.activity_reduce_amount, " +
                "    oi.coupon_reduce_amount, " +
                "    oi.original_total_amount, " +
                "    oi.feight_fee, " +
                "    oi.feight_fee_reduce, " +
                "    oi.refundable_time, " +
                "    oa.id order_detail_activity_id, " +
                "    oa.activity_id, " +
                "    oa.activity_rule_id, " +
                "    oc.id order_detail_coupon_id, " +
                "    oc.coupon_id, " +
                "    oc.coupon_use_id, " +
                "    oi.`type`, " +
                "    oi.`old`, " +
                "    current_row_timestamp() row_op_ts " +
                "from order_detail od " +
                "join order_info oi " +
                "on od.order_id = oi.id " +
                "left join order_detail_activity oa " +
                "on od.id = oa.order_detail_id " +
                "left join order_detail_coupon oc " +
                "on od.id = oc.order_detail_id " +
                "join base_dic FOR SYSTEM_TIME AS OF od.pt as dic " +
                "on od.source_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        /**
         tableEnv.sqlQuery("select * from result_table")
         .execute()
         .print();
         */

        tableEnv.executeSql("" +
                "create table dwd_order_pre( " +
                "    `id` string, " +
                "    `order_id` string, " +
                "    `sku_id` string, " +
                "    `sku_name` string, " +
                "    `order_price` string, " +
                "    `sku_num` string, " +
                "    `create_time` string, " +
                "    `source_type_id` string, " +
                "    `source_type_name` string, " +
                "    `source_id` string, " +
                "    `split_total_amount` string, " +
                "    `split_activity_amount` string, " +
                "    `split_coupon_amount` string, " +
                "    `consignee` string, " +
                "    `consignee_tel` string, " +
                "    `total_amount` string, " +
                "    `order_status` string, " +
                "    `user_id` string, " +
                "    `payment_way` string, " +
                "    `delivery_address` string, " +
                "    `order_comment` string, " +
                "    `out_trade_no` string, " +
                "    `trade_body` string, " +
                "    `operate_time` string, " +
                "    `expire_time` string, " +
                "    `process_status` string, " +
                "    `tracking_no` string, " +
                "    `parent_order_id` string, " +
                "    `province_id` string, " +
                "    `activity_reduce_amount` string, " +
                "    `coupon_reduce_amount` string, " +
                "    `original_total_amount` string, " +
                "    `feight_fee` string, " +
                "    `feight_fee_reduce` string, " +
                "    `refundable_time` string, " +
                "    `order_detail_activity_id` string, " +
                "    `activity_id` string, " +
                "    `activity_rule_id` string, " +
                "    `order_detail_coupon_id` string, " +
                "    `coupon_id` string, " +
                "    `coupon_use_id` string, " +
                "    `type` string, " +
                "    `old` map<string,string>, " +
                "    row_op_ts TIMESTAMP_LTZ(3), " +
                "    primary key(id) not enforced " +
                ")" +
                KafkaUtil.getKafkaUpsertDDL(
                        BROKERS,
                        SINK_TOPIC));

        tableEnv.executeSql("insert into dwd_order_pre select * from result_table");
    }

}
