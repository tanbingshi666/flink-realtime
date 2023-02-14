package com.tan.rt.dwd;

import com.tan.rt.utils.KafkaUtil;
import com.tan.rt.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DwdTradeCartAdd {

    private static final Logger LOG = LoggerFactory.getLogger(DwdTradeCartAdd.class);

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String GROUP_ID = "TRADE-CART-ADD-0121";

    private static final String MYSQL_HOSTNAME = "hadoop102";
    private static final Integer MySQL_PORT = 3306;
    private static final String MYSQL_DATABASE = "gmall";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "128505tan";

    private static final String SINK_TOPIC = "dwd_trade_cart_add";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(KafkaUtil.getTopicDb(BROKERS, GROUP_ID));

        /**
         * (
         *   `database` STRING,
         *   `table` STRING,
         *   `type` STRING,
         *   `data` MAP<STRING, STRING>,
         *   `old` MAP<STRING, STRING>,
         *   `pt` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME*
         * )
         *
         */
        /**
         Table table = tableEnv.sqlQuery("select * from topic_db");
         table.printSchema();

         tableEnv.createTemporaryView("tmp_topic_db", table);
         tableEnv.sqlQuery("select `table`,count(1) as cnt from tmp_topic_db group by `table`")
         .execute()
         .print();
         */

        Table cartAddTable = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] id, " +
                "    `data`['user_id'] user_id, " +
                "    `data`['sku_id'] sku_id, " +
                "    `data`['cart_price'] cart_price, " +
                "    if(`type`='insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num, " +
                "    `data`['sku_name'] sku_name, " +
                "    `data`['is_checked'] is_checked, " +
                "    `data`['create_time'] create_time, " +
                "    `data`['operate_time'] operate_time, " +
                "    `data`['is_ordered'] is_ordered, " +
                "    `data`['order_time'] order_time, " +
                "    `data`['source_type'] source_type, " +
                "    `data`['source_id'] source_id, " +
                "    pt " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'cart_info' " +
                "and (`type` = 'insert' " +
                "or (`type` = 'update'  " +
                "    and  " +
                "    `old`['sku_num'] is not null  " +
                "    and  " +
                "    cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))");

        /**
         tableEnv.createTemporaryView("cart_info", cartAddTable);
         tableEnv.sqlQuery("select * from cart_info")
         .execute()
         .print();
         */

        tableEnv.createTemporaryView("cart_info", cartAddTable);

        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL(
                MYSQL_HOSTNAME,
                MySQL_PORT,
                MYSQL_DATABASE,
                MYSQL_USERNAME,
                MYSQL_PASSWORD
        ));

        /**
         * (
         *   `dic_code` STRING NOT NULL,
         *   `dic_name` STRING,
         *   `parent_code` STRING,
         *   `create_time` TIMESTAMP(6),
         *   `operate_time` TIMESTAMP(6)
         * )
         */
        /**
         Table table = tableEnv.sqlQuery("select * from base_dic");
         table.printSchema();

         tableEnv.createTemporaryView("tmp_base_dic", table);
         tableEnv.sqlQuery("select * from tmp_base_dic")
         .execute()
         .print();
         */

        Table cartAddWithDicTable = tableEnv.sqlQuery("" +
                "select " +
                "    ci.id, " +
                "    ci.user_id, " +
                "    ci.sku_id, " +
                "    ci.cart_price, " +
                "    ci.sku_num, " +
                "    ci.sku_name, " +
                "    ci.is_checked, " +
                "    ci.create_time, " +
                "    ci.operate_time, " +
                "    ci.is_ordered, " +
                "    ci.order_time, " +
                "    ci.source_type source_type_id, " +
                "    dic.dic_name source_type_name, " +
                "    ci.source_id " +
                "from cart_info ci " +
                "join base_dic FOR SYSTEM_TIME AS OF ci.pt as dic " +
                "on ci.source_type = dic.dic_code");

        tableEnv.createTemporaryView("cart_add_dic", cartAddWithDicTable);

        /**
         tableEnv.sqlQuery("select * from cart_add_dic")
         .execute()
         .print();
         */

        tableEnv.executeSql("" +
                "create table dwd_cart_add( " +
                "    `id` STRING, " +
                "    `user_id` STRING, " +
                "    `sku_id` STRING, " +
                "    `cart_price` STRING, " +
                "    `sku_num` STRING, " +
                "    `sku_name` STRING, " +
                "    `is_checked` STRING, " +
                "    `create_time` STRING, " +
                "    `operate_time` STRING, " +
                "    `is_ordered` STRING, " +
                "    `order_time` STRING, " +
                "    `source_type_id` STRING, " +
                "    `source_type_name` STRING, " +
                "    `source_id` STRING " +
                ")" + KafkaUtil.getKafkaSinkDDL(
                BROKERS,
                SINK_TOPIC));

        // {"id":"32358","user_id":"1199","sku_id":"5","cart_price":"999.0","sku_num":"1","sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 明月灰 游戏智能手机 小米 红米","is_checked":null,"create_time":"2023-01-21 10:57:06","operate_time":null,"is_ordered":"0","order_time":null,"source_type_id":"2402","source_type_name":"商品推广","source_id":"13"}
        tableEnv.executeSql("insert into dwd_cart_add select * from cart_add_dic");

        try {
            env.execute("DwdTradeCartAdd Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
