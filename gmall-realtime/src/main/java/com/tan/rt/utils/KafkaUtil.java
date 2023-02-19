package com.tan.rt.utils;

import lombok.NonNull;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;

/**
 * kafka 工具类
 */
public class KafkaUtil {

    private static final String TOPIC_DB = "topic_db";

    /**
     * @param brokers kafka brokers
     * @param topic   消费主题
     * @param groupId 消费者组
     * @param props   优化参数选项
     */
    public static KafkaSource<String> getKafkaSource(@NonNull String brokers,
                                                     @NonNull String topic,
                                                     @NonNull String groupId,
                                                     Properties props) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setProperties(props == null ? new Properties() : props)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    /**
     * @param brokers kafka brokers
     * @param topic   sink topic
     * @param props   优化参数选项
     */
    public static KafkaSink<String> getKafkaSink(@NonNull String brokers,
                                                 @NonNull String topic,
                                                 Properties props) {
        if (props == null) {
            props = new Properties();
        }

        // todo flink kafka 端到端 (EXACTLY_ONCE) 一致性需要开启 checkpoint & state backend
        // todo kafka producer 的参数 transaction.timeout.ms (默认 1h) 必须小于等于 kafka broker 的 transaction.max.timeout.ms (默认 15min)
        // props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, (int) Duration.ofMinutes(10L).toMillis());

        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setKafkaProducerConfig(props)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    /**
     * @param brokers kafka brokers
     * @param groupId 消费者组
     * @return 返回读取 topic_db 主题数据 ddl 语句
     */
    public static String getTopicDb(String brokers,
                                    String groupId) {

        return "CREATE TABLE topic_db ( " +
                "  `database` STRING, " +
                "  `table` STRING, " +
                "  `type` STRING, " +
                "  `data` MAP<STRING,STRING>, " +
                "  `old` MAP<STRING,STRING>, " +
                "  `pt` AS PROCTIME() " +
                ") " +
                getKafkaSourceDDL(
                        brokers,
                        TOPIC_DB,
                        groupId);
    }

    /**
     * @param brokers kafka brokers
     * @param topic   消费主题
     * @param groupId 消费者组
     * @return 返回 kafka source ddl 语句
     */
    public static String getKafkaSourceDDL(@NonNull String brokers,
                                           @NonNull String topic,
                                           @NonNull String groupId) {
        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + brokers + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'earliest-offset'" +
                ")";
    }


    /**
     * @param brokers kafka brokers
     * @param topic   sink 主题
     * @return 返回 Kafka sink ddl 语句
     */
    public static String getKafkaSinkDDL(String brokers,
                                         String topic) {
        return " WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + brokers + "', " +
                "  'format' = 'json'" + ", " +
                "  'sink.delivery-guarantee' = 'at-least-once'" +
                ")";
    }

    /**
     * @param brokers kafka broker
     * @param topic   kafka 主题
     * @return 返回定义 upsert kafka ddl 语句
     */
    public static String getKafkaUpsertDDL(String brokers,
                                           String topic) {
        return " WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + brokers + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }

}