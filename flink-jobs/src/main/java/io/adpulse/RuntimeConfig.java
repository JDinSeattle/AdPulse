package io.adpulse;

import java.time.Duration;
import java.util.Properties;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class RuntimeConfig {
    private RuntimeConfig() {}
    public static String env(String key, String fallback) { return System.getenv().getOrDefault(key, fallback); }
    public static String release() { return env("RELEASE_ID", "live-v1"); }
    public static String topic(String suffix) { return env("TOPIC_PREFIX", "adpulse") + "." + suffix; }
    public static String resultTopic() { return topic("results." + release()); }
    public static StreamExecutionEnvironment environment(String name) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var backend = new EmbeddedRocksDBStateBackend(true);
        backend.setRocksDBOptions(new LocalRocksOptions());
        env.setStateBackend(backend);
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
            org.apache.flink.configuration.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(env("CHECKPOINT_ROOT", "file:///opt/flink/state/checkpoints") + "/" + name);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Duration.ofSeconds(5)));
        return env;
    }
    public static KafkaSource<String> source(String topic, String group, boolean raw) {
        var builder = KafkaSource.<String>builder().setBootstrapServers(env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
            .setTopics(topic).setGroupId(group).setStartingOffsets(OffsetsInitializer.earliest())
            .setProperty("isolation.level", "read_committed");
        if (raw) builder.setDeserializer(new RawDeserializer());
        else builder.setDeserializer(new ValueDeserializer());
        return builder.build();
    }
    public static WatermarkStrategy<String> watermarks(Rules rules) {
        return WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(rules.disorder))
            .withTimestampAssigner((value, previous) -> Json.read(value).path("event_time").asLong())
            .withIdleness(Duration.ofMillis(rules.idle));
    }
    public static KafkaSink<String> sink(String topic, String name, String keyField) {
        Properties props = new Properties();
        props.setProperty("transaction.timeout.ms", "900000");
        return KafkaSink.<String>builder().setBootstrapServers(env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
            .setKafkaProducerConfig(props).setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setTransactionalIdPrefix("adpulse-" + release() + "-" + name + "-")
            .setRecordSerializer((KafkaRecordSerializationSchema<String>) (value, context, timestamp) -> {
                var node = Json.read(value);
                String key = node.path(keyField).asText(node.path("event_id").asText("unknown"));
                return new ProducerRecord<>(topic, key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
            }).build();
    }
    public static class RawDeserializer implements KafkaRecordDeserializationSchema<String> {
        @Override public TypeInformation<String> getProducedType() { return TypeInformation.of(String.class); }
        @Override public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) {
            ObjectNode packet;
            try { packet = (ObjectNode) Json.read(new String(record.value(), StandardCharsets.UTF_8)); }
            catch (Exception error) {
                packet = Json.object(); packet.put("receipt_id", record.topic() + ":" + record.partition() + ":" + record.offset());
                packet.put("received_at", record.timestamp()); packet.put("event", "INVALID_RAW_ENVELOPE");
            }
            var source = packet.putObject("_source");
            source.put("topic", record.topic()); source.put("partition", record.partition()); source.put("offset", record.offset());
            out.collect(packet.toString());
        }
    }
    public static class ValueDeserializer implements KafkaRecordDeserializationSchema<String> {
        @Override public TypeInformation<String> getProducedType() { return TypeInformation.of(String.class); }
        @Override public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) {
            // Debezium emits a null-valued Kafka tombstone after a delete envelope.
            // The delete envelope carries business history; the tombstone is not an event.
            if (record.value() != null) out.collect(new String(record.value(), StandardCharsets.UTF_8));
        }
    }
}
