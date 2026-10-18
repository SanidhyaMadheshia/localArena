package io.localarena.streams;

import io.localarena.events.v1.Envelope;
import io.localarena.events.v1.LeaderboardEntry;
import io.localarena.events.v1.PlayerScored;
import io.localarena.events.v1.PlayerSession;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;

/**
 * LocalArena stream processing (adr/0011).
 *
 * <p>Leaderboard: match-events (key = match) -> PlayerScored re-keyed to
 * "player/match" (repartition) -> KTable of the latest score per player per
 * match (highest tick wins) -> re-grouped by player and summed with an
 * adder/subtractor -> KTable "leaderboard-totals" -> game.leaderboard-updates.v1.
 *
 * <p>The per-match table is what makes this idempotent: PlayerScored carries the
 * running total, so a duplicate or a replayed (older) event cannot change the
 * table, and an updated score replaces the old one through the subtractor
 * instead of being added twice. A naive sum of every PlayerScored would count
 * duplicates.
 *
 * <p>Sessions: player-events + PlayerScored re-keyed by player, session windows
 * with a 30 s inactivity gap and 10 s grace, final results only (suppress).
 */
public final class Topologies {
    public static final String MATCH_EVENTS = "game.match-events.v1";
    public static final String PLAYER_EVENTS = "game.player-events.v1";
    public static final String LEADERBOARD = "game.leaderboard-updates.v1";
    public static final String SESSIONS = "game.player-sessions.v1";
    public static final String TOTALS_STORE = "leaderboard-totals";
    public static final Duration SESSION_GAP = Duration.ofSeconds(30);
    public static final Duration SESSION_GRACE = Duration.ofSeconds(10);

    private Topologies() {}

    public static Topology build() {
        var b = new StreamsBuilder();
        var envelopes = Consumed.with(Serdes.String(), Proto.ENVELOPE).withTimestampExtractor(new Proto.EventTime());
        var scoredSerde = Proto.serde(PlayerScored.parser());

        KStream<String, PlayerScored> scored = b.stream(MATCH_EVENTS, envelopes.withName("match-events"))
                .filter((k, e) -> e != null && e.hasPlayerScored(), Named.as("only-scores"))
                .mapValues(Envelope::getPlayerScored, Named.as("unwrap-score"));

        // 1. Latest score per (player, match). Highest tick wins: replays and
        //    duplicates (same or older tick) are no-ops.
        var perMatch = scored
                .selectKey((k, s) -> s.getPlayerId() + "/" + s.getMatchId(), Named.as("key-by-player-match"))
                .groupByKey(Grouped.with("by-player-match", Serdes.String(), scoredSerde))
                .reduce((cur, next) -> next.getTick() >= cur.getTick() ? next : cur,
                        Materialized.<String, PlayerScored, KeyValueStore<Bytes, byte[]>>as("score-per-match")
                                .withKeySerde(Serdes.String()).withValueSerde(scoredSerde));

        // 2. Season total per player: sum of per-match scores, retracting the old
        //    value of a (player, match) row whenever it is updated.
        var totals = perMatch
                .groupBy((k, s) -> KeyValue.pair(String.valueOf(s.getPlayerId()), (long) s.getScore()),
                        Grouped.with("by-player", Serdes.String(), Serdes.Long()))
                .reduce(Long::sum, (agg, old) -> agg - old,
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(TOTALS_STORE)
                                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));

        totals.toStream(Named.as("totals-changelog"))
                .mapValues((player, total) -> LeaderboardEntry.newBuilder()
                        .setPlayerId(Long.parseLong(player)).setTotalScore(total == null ? 0 : total)
                        .setUpdatedAt(Proto.ts(System.currentTimeMillis())).build(), Named.as("to-entry"))
                .to(LEADERBOARD, Produced.with(Serdes.String(), Proto.serde(LeaderboardEntry.parser())));

        // 3. Sessions: any player activity, keyed by player.
        KStream<String, Envelope> activity = b.stream(PLAYER_EVENTS, envelopes.withName("player-events"))
                .filter((k, e) -> e != null && (e.hasPlayerJoined() || e.hasPlayerLeft()), Named.as("only-presence"));
        activity
                .merge(scored.selectKey((k, s) -> String.valueOf(s.getPlayerId()), Named.as("key-score-by-player"))
                        .mapValues(s -> Envelope.newBuilder().setPlayerScored(s).build(), Named.as("rewrap-score")),
                        Named.as("merge-activity"))
                .groupByKey(Grouped.with("by-player-activity", Serdes.String(), Proto.ENVELOPE))
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(SESSION_GAP, SESSION_GRACE))
                .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("player-sessions")
                        .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()).withName("final-sessions"))
                .toStream(Named.as("closed-sessions"))
                .filter((w, n) -> n != null, Named.as("skip-merged"))
                .map((w, n) -> KeyValue.pair(w.key(), PlayerSession.newBuilder()
                        .setPlayerId(Long.parseLong(w.key())).setStart(Proto.ts(w.window().start()))
                        .setEnd(Proto.ts(w.window().end())).setEvents(n).build()), Named.as("to-session"))
                .to(SESSIONS, Produced.with(Serdes.String(), Proto.serde(PlayerSession.parser())));
        return b.build();
    }
}
