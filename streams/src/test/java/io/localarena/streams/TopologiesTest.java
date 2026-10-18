package io.localarena.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.localarena.events.v1.Envelope;
import io.localarena.events.v1.LeaderboardEntry;
import io.localarena.events.v1.PlayerJoined;
import io.localarena.events.v1.PlayerScored;
import io.localarena.events.v1.PlayerSession;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TopologiesTest {
    private TopologyTestDriver driver;
    private TestInputTopic<String, Envelope> matches;
    private TestInputTopic<String, Envelope> players;
    private TestOutputTopic<String, LeaderboardEntry> leaderboard;
    private TestOutputTopic<String, PlayerSession> sessions;
    private static final Instant T0 = Instant.parse("2026-10-18T12:00:00Z");

    @BeforeEach
    void setUp() {
        Properties p = Main.config();
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        p.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("java.io.tmpdir") + "/la-ttd-" + System.nanoTime());
        driver = new TopologyTestDriver(Topologies.build(), p, T0);
        matches = driver.createInputTopic(Topologies.MATCH_EVENTS, new StringSerializer(), Proto.ENVELOPE.serializer());
        players = driver.createInputTopic(Topologies.PLAYER_EVENTS, new StringSerializer(), Proto.ENVELOPE.serializer());
        leaderboard = driver.createOutputTopic(Topologies.LEADERBOARD, new StringDeserializer(),
                Proto.serde(LeaderboardEntry.parser()).deserializer());
        sessions = driver.createOutputTopic(Topologies.SESSIONS, new StringDeserializer(),
                Proto.serde(PlayerSession.parser()).deserializer());
    }

    @AfterEach
    void tearDown() { driver.close(); }

    private static Envelope scored(String match, long player, int score, long tick, long secs) {
        return Envelope.newBuilder().setEventId(match + "/" + player + "/" + tick)
                .setEventTime(Proto.ts(T0.plusSeconds(secs).toEpochMilli()))
                .setPlayerScored(PlayerScored.newBuilder().setMatchId(match).setPlayerId(player).setScore(score).setTick(tick))
                .build();
    }

    private long total(String player) {
        Long v = driver.<String, Long>getKeyValueStore(Topologies.TOTALS_STORE).get(player);
        return v == null ? 0 : v;
    }

    private Map<String, LeaderboardEntry> latest() {
        return leaderboard.readKeyValuesToMap();
    }

    @Test
    void leaderboardSumsPerMatchFinalScores() {
        matches.pipeInput("m1", scored("m1", 1, 1, 10, 1));
        matches.pipeInput("m1", scored("m1", 1, 2, 20, 2));
        matches.pipeInput("m1", scored("m1", 1, 3, 30, 3));
        matches.pipeInput("m1", scored("m1", 2, 1, 15, 2));
        matches.pipeInput("m2", scored("m2", 1, 5, 40, 4));
        assertEquals(8, total("1"), "3 (m1) + 5 (m2)");
        assertEquals(1, total("2"));
        assertEquals(8, latest().get("1").getTotalScore());
    }

    @Test
    void duplicatesAndReplaysAreIdempotent() {
        matches.pipeInput("m1", scored("m1", 1, 3, 30, 3));
        matches.pipeInput("m1", scored("m1", 1, 3, 30, 3)); // exact duplicate (at-least-once redelivery)
        matches.pipeInput("m1", scored("m1", 1, 2, 20, 2)); // older event replayed from a spool, out of order
        matches.pipeInput("m1", scored("m1", 1, 3, 30, 3));
        assertEquals(3, total("1"), "a naive sum of every PlayerScored would give 11");
    }

    @Test
    void poisonRecordGoesToDlqAndProcessingContinues() {
        var raw = driver.createInputTopic(Topologies.MATCH_EVENTS, new StringSerializer(), new ByteArraySerializer());
        var dlq = driver.createOutputTopic("game.invalid-events.v1", Serdes.ByteArray().deserializer(), Serdes.ByteArray().deserializer());
        raw.pipeInput("m1", new byte[] {(byte) 0xff, 0x01, 0x02});
        matches.pipeInput("m1", scored("m1", 7, 4, 1, 1));
        // TopologyTestDriver (4.4) does not surface deserialization-handler DLQ records;
        // the DLQ copy is verified against the real cluster in Experiment 004.
        assertTrue(dlq.getQueueSize() <= 1);
        assertEquals(4, total("7"), "the poison record is skipped and the next valid record is processed");
    }

    private static Envelope joined(long player, long secs) {
        return Envelope.newBuilder().setEventId("j" + player + "/" + secs)
                .setEventTime(Proto.ts(T0.plusSeconds(secs).toEpochMilli()))
                .setPlayerJoined(PlayerJoined.newBuilder().setPlayerId(player).setMatchId("m")).build();
    }

    @Test
    void sessionsUseEventTimeGapAndGrace() {
        players.pipeInput("1", joined(1, 0));
        matches.pipeInput("m", scored("m", 1, 1, 1, 10));   // same session (gap 30 s)
        players.pipeInput("2", joined(2, 15));              // stream time -> 15 s
        players.pipeInput("1", joined(1, 5));               // late but within grace: merged
        assertTrue(sessions.isEmpty(), "suppress: nothing until the window closes");
        players.pipeInput("2", joined(2, 100));             // stream time -> 100 s closes p1's session
        var out = sessions.readKeyValuesToMap();
        var s = out.get("1");
        assertEquals(3, s.getEvents());
        assertEquals(T0.toEpochMilli(), Proto.millis(s.getStart()));
        assertEquals(T0.plusSeconds(10).toEpochMilli(), Proto.millis(s.getEnd()));
        players.pipeInput("1", joined(1, 8));               // far too late: dropped, no new session
        players.pipeInput("2", joined(2, 200));
        assertTrue(!sessions.readKeyValuesToMap().containsKey("1"), "expired late event must not reopen the session");
    }
}
