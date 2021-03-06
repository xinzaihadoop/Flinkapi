package flink.streamingapi.bean;

/**
 * @ClassName: SensorReader
 * @Description: 消息监控实体类
 * @Author: MovieBook_xinll
 * @Date: 2021/5/19 14:13
 * @Version: v1.0
 */
public class SensorReader {
    private String id;                   //主键ID
    private Long timeStamp;              //时间戳信息
    private Double temperature;          //温度

    public SensorReader() {
    }

    public SensorReader(String id, Long timeStamp, Double temperature) {
        this.id = id;
        this.timeStamp = timeStamp;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReader{" +
                "id='" + id + '\'' +
                ", timeStamp=" + timeStamp +
                ", temperature=" + temperature +
                '}';
    }
}
