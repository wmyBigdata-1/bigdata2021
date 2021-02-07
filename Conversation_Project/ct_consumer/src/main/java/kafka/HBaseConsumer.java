package kafka;/*
 *Author：情深，骚明 and 情骚
 *Version：2019/12/20 & 1.0
 */

import hbase.HBaseDAO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.PropertiesUtils;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

public class HBaseConsumer {
    public static void main(String[] args) throws IOException, ParseException {
        //kafka消费者的新API
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(PropertiesUtils.properties);
        consumer.subscribe(Arrays.asList(PropertiesUtils.getProperty("kafka.topics")));

        //从HBASE过来，放入数据
        HBaseDAO hBaseDAO = new HBaseDAO();


        //topic数据循环写数据
        while (true) {
            //使用内网来用的，保证数据安全
            //拉取数据的时候休息0.1秒
            ConsumerRecords<String, String> poll = consumer.poll(100);

            for (ConsumerRecord<String, String> cr : poll) {
                String value = cr.value();
                System.out.println(value);
                hBaseDAO.put(value);
            }
        }
    }
}
