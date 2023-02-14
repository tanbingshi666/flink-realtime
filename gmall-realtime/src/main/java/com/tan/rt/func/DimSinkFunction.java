package com.tan.rt.func;

import cn.hutool.json.JSONObject;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.tan.rt.utils.DruidUtil;
import com.tan.rt.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private DruidDataSource druidDataSource = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.druidDataSource = DruidUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject json, Context context) throws Exception {
        // {"database":"gmall","table":"user_info","type":"bootstrap-insert","ts":1674280976,"data":{"id":376,"login_name":"nq5vp57n0k1j","name":"纪嘉","user_level":"2","birthday":"1972-10-18","gender":"F","create_time":"2023-01-18 10:32:27"},"sinkTable":"dim_user_info"}

        DruidPooledConnection connection = druidDataSource.getConnection();

        String sinkTable = json.getStr("sinkTable");
        JSONObject data = json.getJSONObject("data");

        String type = json.getStr("type");
        // todo 如果为更新数据,则需要删除 Redis 中的数据

        PhoenixUtil.upsertValues(connection, sinkTable, data);
        connection.close();
    }
}
