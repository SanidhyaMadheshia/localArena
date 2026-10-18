-- Seed data for Experiment 003 (idempotent). Run with:
--   docker compose exec -T postgres psql -U localarena -d localarena -v ON_ERROR_STOP=1 -f - < experiments/003-week3-resilience/seed.sql
SET statement_timeout = 0;
INSERT INTO players (username) SELECT 'load' || g FROM generate_series(1, 1000) g ON CONFLICT (username) DO NOTHING;
INSERT INTO profiles (player_id, display_name, credits)
  SELECT id, username, 1000000 FROM players WHERE username ~ '^load[0-9]+$' ON CONFLICT DO NOTHING;
DO $$
DECLARE lo bigint; hi bigint;
BEGIN
  SELECT min(id), max(id) INTO lo, hi FROM players WHERE username ~ '^load[0-9]+$';
  IF (SELECT count(*) FROM matches) < 20000 THEN
    INSERT INTO matches (started_at, ended_at)
      SELECT ts, ts + interval '5 min'
      FROM (SELECT timestamptz '2026-10-01' + g * interval '60 seconds' AS ts FROM generate_series(1, 20000) g) x;
    INSERT INTO match_players (match_id, started_at, player_id, score)
      SELECT m.id, m.started_at, lo + ((m.id * 7 + k * 97) % (hi - lo + 1)), (m.id + k) % 50
      FROM matches m CROSS JOIN generate_series(0, 9) k
      ON CONFLICT DO NOTHING;
  END IF;
END $$;
ANALYZE;
SELECT min(id) || ' ' || max(id) FROM players WHERE username ~ '^load[0-9]+$';
