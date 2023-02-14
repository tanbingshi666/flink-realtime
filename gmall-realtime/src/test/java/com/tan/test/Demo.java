package com.tan.test;

import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo {

    public static void main(String[] args) {

        /**
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setParallelism(1);

         env.socketTextStream("hadoop102", 8899).print();

         try {
         env.execute("demo");
         } catch (Exception e) {
         e.printStackTrace();
         }
         */

        String line = "{\"actions\":[{\"action_id\":\"trade_add_address\",\"ts\":1674280123083}],\"common\":{\"ar\":\"310000\",\"ba\":\"Xiaomi\",\"ch\":\"wandoujia\",\"is_new\":\"0\",\"md\":\"Xiaomi Mix2 \",\"mid\":\"mid_676544\",\"os\":\"Android 10.0\",\"uid\":\"887\",\"vc\":\"v2.1.132\"},\"page\":{\"during_time\":14166,\"item\":\"31\",\"item_type\":\"sku_ids\",\"last_page_id\":\"cart\",\"page_id\":\"trade\"},\"ts\":1674280116000}";

        /**
         JSON json = JSONUtil.parse(line);
         System.out.println(json.getByPath("hello"));
         System.out.println(json);
         */

        /**
         JSONObject json = (JSONObject) JSONUtil.parse(line);
         String mid = json.getByPath("common.mid", String.class);
         System.out.println(mid);
         */

        /**
         JSONObject json = (JSONObject) JSONUtil.parse(line);
         json.getJSONObject("common").set("is_new", "1");
         System.out.println(json.toJSONString(0));
         */

        JSONObject json = (JSONObject) JSONUtil.parse(line);
        json.putByPath("common.is_new", "1");
        System.out.println(json.toJSONString(0));


    }

}
