package com.tan.rt.utils;

import com.tan.rt.bean.TransientSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;

public class ClickHouseUtil {

    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/gmall";

    public static <T> SinkFunction<T> getSinkFunction(String sql) {
        return JdbcSink.sink(sql,
                (JdbcStatementBuilder<T>) (preparedStatement, t) -> {

                    Class<?> tClz = t.getClass();

                    Field[] declaredFields = tClz.getDeclaredFields();
                    int offset = 0;
                    for (int i = 0; i < declaredFields.length; i++) {

                        Field field = declaredFields[i];
                        field.setAccessible(true);

                        TransientSink transientSink = field.getAnnotation(TransientSink.class);
                        if (transientSink != null) {
                            offset++;
                            continue;
                        }

                        Object value = null;
                        try {
                            value = field.get(t);
                            preparedStatement.setObject(i + 1 - offset, value);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }

                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(CLICKHOUSE_DRIVER)
                        .withUrl(CLICKHOUSE_URL)
                        .build());
    }

}
