package com.testapp.kafka;

import java.util.Arrays;
import java.util.List;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaRebalanceListener {

  @Autowired
  private ConsumerFactory<Object, Object> consumerFactory;

  @PostConstruct
  @SuppressWarnings("unchecked")
  public void onPartitionsAssigned() {

    Consumer<Object, Object> consumer = consumerFactory.createConsumer();
    String topicName = "com.avaloq.acp.bde.obj_asset";
    List<TopicPartition> partitions = Arrays.asList(
        new TopicPartition(topicName, 0),
        new TopicPartition(topicName, 1),
        new TopicPartition(topicName, 2),
        new TopicPartition(topicName, 3),
        new TopicPartition(topicName, 4),
        new TopicPartition(topicName, 5),
        new TopicPartition(topicName, 6),
        new TopicPartition(topicName, 7),
        new TopicPartition(topicName, 8),
        new TopicPartition(topicName, 9));

    consumer.assign(partitions);

    consumer.seekToBeginning(partitions);
    consumer.commitSync();

    partitions.forEach(partition -> {
      long offset = consumer.committed(partition).offset();

      while (offset != 0) {
        log.info("Partition : {}, offset {}", partition, offset);
        offset = consumer.committed(partition).offset();

        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    consumer.unsubscribe();
    consumer.close();
  }
}
