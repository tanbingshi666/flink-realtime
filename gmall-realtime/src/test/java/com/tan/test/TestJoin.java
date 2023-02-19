package com.tan.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.Serializable;
import java.time.Duration;

public class TestJoin {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //1001,23.6,1324
        SingleOutputStreamOperator<WaterSensor> waterSensorDS1 = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Double.parseDouble(split[1]),
                            Long.parseLong(split[2]));
                }).returns(WaterSensor.class);

        SingleOutputStreamOperator<WaterSensor2> waterSensorDS2 = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor2(split[0],
                            split[1],
                            Long.parseLong(split[2]));
                }).returns(WaterSensor2.class);

        SingleOutputStreamOperator<WaterSensor3> waterSensorDS3 = env.socketTextStream("hadoop102", 10000)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor3(split[0],
                            split[1],
                            Long.parseLong(split[2]));
                }).returns(WaterSensor3.class);

        //将流转换为动态表
        tableEnv.createTemporaryView("t1", waterSensorDS1);
        tableEnv.createTemporaryView("t2", waterSensorDS2);
        tableEnv.createTemporaryView("t3", waterSensorDS3);

        //FlinkSQLJOIN

        //inner join  左表:OnCreateAndWrite   右表:OnCreateAndWrite
        tableEnv.sqlQuery("select " +
                        "t1.id, " +
                        "t1.vc, " +
                        "t2.id, " +
                        "t2.name, " +
                        "t3.id, " +
                        "t3.name " +
                        "from t1 join t2 on t1.id = t2.id " +
                        "left join t3 on t1.id = t3.id")
                .execute()
                .print();

        //left join   左表:OnReadAndWrite     右表:OnCreateAndWrite
        //tableEnv.sqlQuery("select t1.id,t1.vc,t2.id,t2.name from t1 left join t2 on t1.id=t2.id")
        //        .execute()
        //        .print();

        //right join  左表:OnCreateAndWrite   右表:OnReadAndWrite
        //tableEnv.sqlQuery("select t1.id,t1.vc,t2.id,t2.name from t1 right join t2 on t1.id=t2.id")
        //        .execute()
        //        .print();

        //full join   左表:OnReadAndWrite     右表:OnReadAndWrite
        //tableEnv.sqlQuery("select t1.id,t1.vc,t2.id,t2.name from t1 full join t2 on t1.id=t2.id")
        //       .execute()
        //       .print();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WaterSensor implements Serializable {
        public String id;
        public double vc;
        public long ts;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WaterSensor2 implements Serializable {
        public String id;
        public String name;
        public long ts;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WaterSensor3 implements Serializable {
        public String id;
        public String name;
        public long ts;
    }

}

