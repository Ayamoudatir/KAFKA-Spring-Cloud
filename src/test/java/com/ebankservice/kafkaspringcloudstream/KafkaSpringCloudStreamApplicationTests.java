package com.ebankservice.kafkaspringcloudstream;

import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {"T2", "T3", "T4"})
class KafkaSpringCloudStreamApplicationTests {

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @LocalServerPort
    private int port;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.cloud.stream.kafka.binder.brokers",
                () -> System.getProperty("spring.embedded.kafka.brokers"));
        registry.add("spring.cloud.stream.kafka.streams.binder.brokers",
                () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void contextLoads() {
        assertThat(rest.getForEntity("/index.html", String.class).getStatusCode().is2xxSuccessful()).isTrue();
    }

    @Test
    void publishEndpointAcceptsAnEvent() {
        var response = rest.getForEntity("/publish?name=integration-test&topic=T2", String.class);

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).contains("integration-test");
    }

    @Test
    void supplierEventsAreAggregatedAndWrittenToT4() {
        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps("analytics-test", "false", embeddedKafka);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var consumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new LongDeserializer())) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "T4");
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            ConsumerRecord<String, Long> result = null;

            while (System.nanoTime() < deadline && result == null) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (var record : records) {
                    if (("P1".equals(record.key()) || "P2".equals(record.key())) && record.value() > 0) {
                        result = record;
                        break;
                    }
                }
            }

            assertThat(result).as("Kafka Streams should publish a count to T4").isNotNull();
        }
    }

    @Test
    void analyticsEndpointStreamsServerSentEvents() throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/analytics")).GET().build();
        var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofInputStream());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("content-type").orElse("")).startsWith("text/event-stream");
        try (var reader = new BufferedReader(new InputStreamReader(response.body()))) {
            String line;
            do {
                line = reader.readLine();
            } while (line != null && !line.startsWith("data:"));
            assertThat(line).startsWith("data:");
        }
    }
}
