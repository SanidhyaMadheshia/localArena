package io.localarena.streams;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.google.protobuf.Timestamp;
import io.localarena.events.v1.Envelope;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.TimestampExtractor;

/** Protobuf serdes and event-time extraction shared by all topologies. */
final class Proto {
    private Proto() {}

    static <T extends Message> Serde<T> serde(Parser<T> parser) {
        Serializer<T> ser = (topic, msg) -> msg == null ? null : msg.toByteArray();
        Deserializer<T> de = (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                return parser.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                // Surfaces to the deserialization exception handler -> DLQ (game.invalid-events.v1).
                throw new SerializationException("not a valid " + topic + " record", e);
            }
        };
        return Serdes.serdeFrom(ser, de);
    }

    static final Serde<Envelope> ENVELOPE = serde(Envelope.parser());

    static Timestamp ts(long epochMillis) {
        return Timestamp.newBuilder().setSeconds(Math.floorDiv(epochMillis, 1000))
                .setNanos((int) Math.floorMod(epochMillis, 1000) * 1_000_000).build();
    }

    static long millis(Timestamp t) {
        return Instant.ofEpochSecond(t.getSeconds(), t.getNanos()).toEpochMilli();
    }

    /**
     * Event time = Envelope.event_time (the game tick time), not the Kafka record
     * time: a spooled event replayed minutes later still lands in the right window.
     * Falls back to the record timestamp for anything that is not an Envelope.
     */
    static final class EventTime implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            if (record.value() instanceof Envelope e && e.hasEventTime()) {
                return millis(e.getEventTime());
            }
            return record.timestamp() >= 0 ? record.timestamp() : partitionTime;
        }
    }
}
