package com.tan.rt.dwd;

import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DwdTradeCancelOrderDetail {

    private static final Logger LOG = LoggerFactory.getLogger(DwdTradeCancelOrderDetail.class);

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "dwd_trade_order_pre_process";
    private static final String GROUP_ID = "TRADE-CANCEL-ORDER-DETAIL-0121";

    private static final String SINK_TOPIC = "dwd_trade_cancel_order_detail";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "create table dwd_order_pre_process( " +
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
                        "    `row_op_ts` TIMESTAMP_LTZ(3) " +
                        ")" +
                        KafkaUtil.getKafkaSourceDDL(BROKERS,
                                SOURCE_TOPIC,
                                GROUP_ID
                        ));

        /**
         tableEnv.sqlQuery("select * from dwd_order_pre_process")
         .execute()
         .print();
         */

        Table cancelOrderDetail = tableEnv.sqlQuery(
                "select " +
                        "id, " +
                        "order_id, " +
                        "user_id, " +
                        "sku_id, " +
                        "sku_name, " +
                        "province_id, " +
                        "activity_id, " +
                        "activity_rule_id, " +
                        "coupon_id, " +
                        "operate_time cancel_time, " +
                        "source_id, " +
                        "source_type_id, " +
                        "source_type_name, " +
                        "sku_num, " +
                        "order_price, " +
                        "split_activity_amount, " +
                        "split_coupon_amount, " +
                        "split_total_amount " +
                        "from dwd_order_pre_process " +
                        "where `type` = 'update' " +
                        "and `old`['order_status'] is not null " +
                        "and order_status = '1003'");
        tableEnv.createTemporaryView("cancel_order_detail", cancelOrderDetail);

        /**
         tableEnv.sqlQuery("select * from cancel_order_detail")
         .execute()
         .print();
         */

        tableEnv.executeSql("create table dwd_trade_cancel_order_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "cancel_time string, " +
                "source_id string, " +
                "source_type_id string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "order_price string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string " +
                ") " +
                KafkaUtil.getKafkaSinkDDL(BROKERS, SINK_TOPIC));

        tableEnv.executeSql("insert into dwd_trade_cancel_order_detail select * from cancel_order_detail");

    }

}
