package com.testapp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableScheduling;

@PropertySource(value = "classpath:META-INF/build-info.properties", ignoreResourceNotFound = true)
@SpringBootApplication
@EnableScheduling
public class KafkaApplication {

  public static void main(String[] args) {
    SpringApplication.run(KafkaApplication.class, args);
  }
}
