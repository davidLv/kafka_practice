package practice;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class ProcessingApp {

    public static void main(String[] args) {
        String servers = args[0];
        String groupId = args[1];
        String sourceTopic = args[2];
        String targetTopic = args[3];

        Reader reader = new Reader(servers, groupId, sourceTopic);
        Writer writer = new Writer(servers, targetTopic);

        while (true) { // 1
            ConsumerRecords<String, String> consumeRecords = reader.consume();
            for (ConsumerRecord<String, String> record : consumeRecords) {
                writer.produce(record.value()); // 2
            }
        }
    }
}