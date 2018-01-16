package practice;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class ProcessingApp {

    public static void main(String[] args) {
        String servers = args[0];
        String groupId = args[1];
        String sourceTopic = args[2];
        String goodTopic = args[3];
        String badTopic = args[4];

        Reader reader = new Reader(servers, groupId, sourceTopic);
        Validator validator = new Validator(servers, goodTopic, badTopic);

        while (true) {
            ConsumerRecords<String, String> consumeRecords = reader.consume();
            for (ConsumerRecord<String, String> record : consumeRecords) {
                validator.produce(record.value());
            }
        }
    }
}