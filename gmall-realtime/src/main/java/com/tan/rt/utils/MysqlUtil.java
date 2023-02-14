package com.tan.rt.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.NonNull;

import java.util.Properties;

/**
 * mysql 工具类
 */
public class MysqlUtil {

    private static final String BASE_DIC_TABLE = "base_dic";

    /**
     * @param hostname   mysql 地址
     * @param port       端口
     * @param username   用户名
     * @param password   密码
     * @param databases  数据库
     * @param tables     表名
     * @param mysqlProps 优化参数选型
     */
    public static MySqlSource<String> getMySqlSource(@NonNull String hostname,
                                                     @NonNull Integer port,
                                                     @NonNull String username,
                                                     @NonNull String password,
                                                     @NonNull String[] databases,
                                                     @NonNull String[] tables,
                                                     Properties mysqlProps
    ) {
        return MySqlSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(password)
                .databaseList(databases)
                .tableList(tables)
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(mysqlProps == null ? new Properties() : mysqlProps)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
    }

    /**
     * @return 定义 mysql base_dic 表 ddl 语句
     */
    public static String getBaseDicLookUpDDL(@NonNull String hostname,
                                             @NonNull Integer port,
                                             @NonNull String database,
                                             @NonNull String username,
                                             @NonNull String password) {
        return "create table `base_dic`( " +
                "`dic_code` string, " +
                "`dic_name` string, " +
                "`parent_code` string, " +
                "`create_time` timestamp, " +
                "`operate_time` timestamp, " +
                "primary key(`dic_code`) not enforced " +
                ")" +
                MysqlUtil.lookUpTableDDL(hostname,
                        port,
                        database,
                        BASE_DIC_TABLE,
                        username,
                        password);
    }

    /**
     * @param hostname  mysql 主机
     * @param port      mysql 端口
     * @param database  mysql 数据库
     * @param tableName mysql 表名
     * @param username  mysql 用户名
     * @param password  mysql 密码
     * @return 返回 mysql table 通用 look up ddl 语句
     */
    public static String lookUpTableDDL(@NonNull String hostname,
                                        @NonNull Integer port,
                                        @NonNull String database,
                                        @NonNull String tableName,
                                        @NonNull String username,
                                        @NonNull String password) {

        return " WITH ( " +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://" + hostname + ":" + port + "/" + database + "', " +
                "'table-name' = '" + tableName + "', " +
                "'lookup.cache' = 'PARTIAL', " +
                "'lookup.partial-cache.max-rows' = '10', " +
                "'lookup.partial-cache.expire-after-write' = '60s', " +
                // todo lookup.partial-cache.caching-missing-key 参数值如何配置 (官网说明值类型为 Boolean, 配置 true 或者 false 都出错)
                //"'lookup.partial-cache.caching-missing-key' = false, " +
                "'username' = '" + username + "', " +
                "'password' = '" + password + "', " +
                "'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
    }

}
