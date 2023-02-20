package com.tan.rt.dws;

import com.tan.rt.bean.KeywordBean;
import com.tan.rt.func.KeywordFunction;
import com.tan.rt.utils.ClickHouseUtil;
import com.tan.rt.utils.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow {

    private static final String BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String SOURCE_TOPIC = "dwd_traffic_page_log";
    private static final String GROUP_ID = "dws_traffic_source_keyword_page_view_window_0121";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "create table page_log( " +
                        "    `page` map<string,string>, " +
                        "    `ts` bigint, " +
                        "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000)), " +
                        "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                        " ) " + KafkaUtil.getKafkaSource(BROKERS, SOURCE_TOPIC, GROUP_ID, null));

        Table pageLog = tableEnv.sqlQuery("" +
                " select " +
                "    page['item'] item, " +
                "    rt " +
                " from page_log " +
                " where page['last_page_id'] = 'search' " +
                " and page['item_type'] = 'keyword' " +
                " and page['item'] is not null");
        tableEnv.createTemporaryView("page_log", pageLog);

        tableEnv.createTemporarySystemFunction("KeywordFunction", KeywordFunction.class);
        Table keywordTable = tableEnv.sqlQuery("" +
                "SELECT " +
                "    word, " +
                "    rt " +
                "FROM filter_table,  " +
                "LATERAL TABLE(KeywordFunction(item))");
        tableEnv.createTemporaryView("keyword_table", keywordTable);

        Table resultTable = tableEnv.sqlQuery(
                "select " +
                        "    'search' source, " +
                        "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                        "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                        "    word keyword, " +
                        "    count(*) keyword_count, " +
                        "    UNIX_TIMESTAMP()*1000 ts " +
                        "from keyword_table " +
                        "group by word,TUMBLE(rt, INTERVAL '10' SECOND)");

        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toDataStream(resultTable, KeywordBean.class);

        keywordBeanDataStream.addSink(ClickHouseUtil.getSinkFunction("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));

        try {
            env.execute("DwsTrafficSourceKeywordPageViewWindow Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
