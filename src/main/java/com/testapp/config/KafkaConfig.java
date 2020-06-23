package com.testapp.config;

import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler;
import org.springframework.kafka.support.LoggingProducerListener;

@Configuration
@EnableKafka
@RequiredArgsConstructor
@Slf4j
public class KafkaConfig {

  @Bean
  public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
      ConcurrentKafkaListenerContainerFactoryConfigurer configurer) {
    final ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    configurer.configure(factory, consumerFactory());
    factory.setConcurrency(1);
    factory.setBatchListener(true);
    factory.getContainerProperties().setPollTimeout(1_000);
    factory.getContainerProperties().setIdleEventInterval(1_000L);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
    factory.getContainerProperties().setAckOnError(false);
    factory.setBatchErrorHandler(new SeekToCurrentBatchErrorHandler());
    factory.setAutoStartup(false);

    return factory;
  }

  @Bean
  @Lazy
  public KafkaTemplate<String, byte[]> kafkaTemplate() {
    final LoggingProducerListener<String, byte[]> listener = new LoggingProducerListener<>();
    // Reading content of messages just for logging purposes causes is wasteful
    listener.setIncludeContents(false);
    final KafkaTemplate<String, byte[]> template = new KafkaTemplate(producerFactory());
    template.setProducerListener(listener);
    return template;
  }

  @Bean
  public ProducerFactory<Object, Object> producerFactory() {
    return new DefaultKafkaProducerFactory<>(producerConfigs());
  }

  @Bean
  public Map<String, Object> producerConfigs() {
    Map<String, Object> props = new HashMap<>(commonConfigs());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(ProducerConfig.ACKS_CONFIG, "-1");  // -1 = all
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    return props;
  }

  @Bean
  public ConsumerFactory<Object, Object> consumerFactory() {
    return new DefaultKafkaConsumerFactory<>(consumerConfigs());
  }

  @Bean
  public Map<String, Object> consumerConfigs() {
    Map<String, Object> props = new HashMap<>(commonConfigs());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    // This size was get by an empirical way. So if you think this is wrong, feel free to update it
    props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 65536);
    return props;
  }

  @Bean
  @Qualifier("kafka.common.config")
  public Map<String, Object> commonConfigs() {
    Map<String, Object> props = new HashMap<>();

    // BOOTSTRAP SERVERS
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        "");

    // I use SSL, will omit it for the test
    return props;
  }
}