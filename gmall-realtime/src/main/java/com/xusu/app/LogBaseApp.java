package com.xusu.app;

import com.alibaba.fastjson.JSONObject;
import com.xusu.utils.KafkaConnUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

public class LogBaseApp {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境，并设置并行度，开启CK，设置状态后端（HDFS）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 为kafka主题的分区数
        //1.1设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs:hadoop102:8020/gmall/flink/dwd_log"));
        //1.2开启ck
        env.enableCheckpointing(100000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.读取Kafa ods_base_log 主题数据
        DataStreamSource<String> kafkaDS = env.addSource(KafkaConnUtil.getKafkaSource("ods_base_log", "dwd_log"));
        //3.转换为JSONObject
        SingleOutputStreamOperator<JSONObject> dataMap = kafkaDS.map((JSONObject::parseObject));
        //4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = dataMap.keyBy(data -> data.getJSONObject("common").getString("mid"));
        //5.按照状态做新老用户校验
        SingleOutputStreamOperator<JSONObject> isNewMap = keyedStream.map(new NewMidRichMapFunc());
        //6.分流，使用ProcessFunction将ODS层数据拆分启动，曝光以及页面数据。
        SingleOutputStreamOperator<String> process = isNewMap.process(new splitProcessFunc());
        //7.将三个流写入Kafka主题
        DataStream<String> startSideOutput = process.getSideOutput(new OutputTag<String>("start") {
        });
        DataStream<String> dispalySideOutput = process.getSideOutput(new OutputTag<String>("display") {
        });
        startSideOutput.addSink(KafkaConnUtil.getKafkaSink("dwd_start_log"));
        dispalySideOutput.addSink(KafkaConnUtil.getKafkaSink("dwd_display_log"));
        process.addSink(KafkaConnUtil.getKafkaSink("dwd_page_log"));
        //8.执行任务
        env.execute();
    }

    private static class NewMidRichMapFunc extends RichMapFunction<JSONObject, JSONObject> {
        //声明状态用于表示当前Mid是否已经访问过
        private ValueState<String> firstVisitDataState;
        private SimpleDateFormat simpleDateFormat;

        @Override
        public void open(Configuration parameters) throws Exception {
            firstVisitDataState = getRuntimeContext().getState(new ValueStateDescriptor<String>("new_id",String.class));
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        }

        @Override
        public JSONObject map(JSONObject jsonObject) throws Exception {

            //获取新用户标识
            String isNew = jsonObject.getJSONObject("common").getString("is_new");
            //如果当前传输数据为新用户，则校验
            if ("1".equals(isNew)){
                Long ts = jsonObject.getLong("ts");
                if (firstVisitDataState.value() != null) {
                    jsonObject.getJSONObject("commit").put("is_new", 1);
                } else {
                    firstVisitDataState.update(simpleDateFormat.format(ts));
                }
            }
            return jsonObject;
        }
    }

    private static class splitProcessFunc extends ProcessFunction<JSONObject, String> {
        @Override
        public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {

            //提取"start"字段
            String startStr = jsonObject.getString("start");
            if (startStr != null && startStr.length() > 0){
                context.output(new OutputTag<String>("start"){},jsonObject.toString());
            }else {
                JSONObject displays = jsonObject.getJSONObject("displays");
                if (displays != null && displays.size() > 0){
                    //遍历曝光数据，写入侧输出流
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject displayJson = displays.getJSONObject(String.valueOf(i));
                        displayJson.put("page_id",jsonObject.getJSONObject("page").getString("page_id"));
                        context.output(new OutputTag<String>("display"){},displayJson.toString());
                    }
                    context.output(new OutputTag<String>("displays"){},jsonObject.toString());
                }else {
                    //页面数据
                    collector.collect(jsonObject.toString());
                }
            }
        }
    }
}
