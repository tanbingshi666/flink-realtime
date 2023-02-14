package com.tan.rt.utils;

import cn.hutool.json.JSONObject;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.tan.rt.common.Constants;
import org.apache.commons.lang3.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {

    /**
     * @param connection Phoenix 连接
     * @param sinkTable  表名   tn
     * @param data       数据   {"id":"1001","name":"zhangsan","sex":"male"}
     */
    public static void upsertValues(DruidPooledConnection connection,
                                    String sinkTable,
                                    JSONObject data) throws SQLException {

        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();

        String sql = "upsert into " + Constants.HBASE_SCHEMA + "." + sinkTable +
                "(" + StringUtils.join(columns, ",") + ") values ('" +
                StringUtils.join(values, "','") + "')";

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.execute();
        connection.commit();

        preparedStatement.close();
    }

}
