package flink.streamingapi.sink;

import flink.streamingapi.bean.SensorReader;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @ClassName: SinkTest02_Redis
 * @Description: TODO
 * @Author: MovieBook_xinll
 * @Date: 2021/5/24 16:19
 * @Version: v1.0
 */
public class SinkTest02_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> sourceDS = env.readTextFile("D:\\MovieBook_Idea\\IdeaProject\\MovieBook\\src\\resources\\Sensor2.md");
        SingleOutputStreamOperator<SensorReader> srMapStream = sourceDS.map(lines -> {
            String[] split = lines.split(" ");
            return new SensorReader(split[0], new Long(split[1]), new Double(split[2]));
        });
        //声明连接配置信息
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("test01")
                .setPort(6379)
                .build();

        DataStreamSink<SensorReader> redisSink = srMapStream.addSink(new RedisSink<SensorReader>(conf, new MyRedisMap()));
        env.execute();
    }

    public static class MyRedisMap implements RedisMapper<SensorReader> {
        /**
         * @Description: getCommandDescription方法是用来自定义保存数据进redis的命令
         * @param: []
         *
         * @return: org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription
         * @auther: MovieBook_xinll
         * @date: 2021/5/24 16:58
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "redis_xin");
        }

        /**
         * @Description: getKeyFromData方法是 获取key值
         * @param: [s]
         * @return: java.lang.String
         * @auther: MovieBook_xinll
         * @date: 2021/5/24 17:02
         */
        @Override
        public String getKeyFromData(SensorReader s) {
            return s.getId();
        }

        /**
         * @Description: getValueFromData方法是 获取value值
         * @param: [s]
         * @return: java.lang.String
         * @auther: MovieBook_xinll
         * @date: 2021/5/24 17:02
         */
        @Override
        public String getValueFromData(SensorReader s) {
            return s.getTimeStamp().toString();
        }
    }
}
