package com.tan.rt.func;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.tan.rt.bean.TableProcess;
import com.tan.rt.common.Constants;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private static final Logger LOG = LoggerFactory.getLogger(TableProcessFunction.class);

    private Connection connection;
    private final MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.connection = DriverManager.getConnection(Constants.PHOENIX_SERVER);

        // todo 读取一次 db 广播流全量数据避免匹配误差
    }

    @Override
    public void processBroadcastElement(String line,
                                        BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context,
                                        Collector<JSONObject> out) throws Exception {

        // {"before":null,"after":{"source_table":"user_info","sink_table":"dim_user_info","sink_columns":"id,login_name,name,user_level,birthday,gender,create_time,operate_time","sink_pk":"id","sink_extend":" SALT_BUCKETS = 3"},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1676046543000,"snapshot":"false","db":"gmall_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000006","pos":4139,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1676046544184,"transaction":null}
        JSONObject json = (JSONObject) JSONUtil.parse(line);
        TableProcess tableProcess = json.get("after", TableProcess.class);

        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());

        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(), tableProcess);
    }

    @Override
    public void processElement(JSONObject line,
                               BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext context,
                               Collector<JSONObject> out) throws Exception {

        ReadOnlyBroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);

        //{"database":"gmall","table":"user_info","type":"bootstrap-insert","ts":1674281295,"data":{"id":209,"login_name":"es1q2mo","nick_name":"阿士","passwd":null,"name":"杨军","phone_num":"13136543791","email":"es1q2mo@qq.com","head_img":null,"user_level":"2","birthday":"1996-06-18","gender":"M","create_time":"2023-01-18 10:32:27","operate_time":"2023-01-19 10:33:17","status":null}}
        String table = line.getStr("table");
        TableProcess tableProcess = broadcastState.get(table);

        if (tableProcess != null) {

            filterColumn(line.getJSONObject("data"), tableProcess.getSinkColumns());
            line.set("sinkTable", tableProcess.getSinkTable());
            out.collect(line);
        } else {
            LOG.info("找不到对应的Key：{}", table);
        }

    }

    /**
     * 校验并建表:
     * create table if not exists db.tn(
     * id varchar primary key,
     * bb varchar,
     * cc varchar
     * ) xxx
     *
     * @param sinkTable   Phoenix 表名
     * @param sinkColumns Phoenix 表字段
     * @param sinkPk      Phoenix 表主键
     * @param sinkExtend  Phoenix 表扩展字段
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            if (StringUtils.isNullOrWhitespaceOnly(sinkPk)) {
                sinkPk = "id";
            }

            if (StringUtils.isNullOrWhitespaceOnly(sinkExtend)) {
                sinkExtend = "";
            }

            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(Constants.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                if (sinkPk.equals(column)) {
                    createTableSql.append(column).append(" varchar primary key");
                } else {
                    createTableSql.append(column).append(" varchar");
                }

                if (i < columns.length - 1) {
                    createTableSql.append(",");
                }
            }

            createTableSql.append(")").append(sinkExtend);

            LOG.info("建表语句为：{}", createTableSql);
            preparedStatement = connection.prepareStatement(createTableSql.toString());

            preparedStatement.execute();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表失败：" + sinkTable);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 过滤字段
     *
     * @param data        {"id":13,"tm_name":"atguigu","logo_url":"/bbb/bbb"}
     * @param sinkColumns "id,tm_name"
     */
    private void filterColumn(JSONObject data, String sinkColumns) {

        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));
    }

}
