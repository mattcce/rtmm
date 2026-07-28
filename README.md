# rt-matchmaking

Real-time skill-based matchmaking engine.

## Quickstart

```bash
# Build
cargo build --release

# Run all three processes (Ctrl+C to stop)
./run-all.sh
```

## Architecture

```
Generator ──/tmp/rtmm.in.sock──▶ Matchmaker ──/tmp/rtmm.out.sock──▶ Validator
    │                                    │                              │
    │  Produces batches of               │  Discretises rating          │  Computes match
    │  MatchmakingRequest                │  space into 50 buckets.      │  quality metrics:
    │  sampled from N(1000,200²).        │  Scheduler hands tickets     │  rating variability
    │                                    │  to N workers. Workers       │  penalty + response
    │                                    │  match groups of 10 from     │  time.
    │                                    │  contiguous buckets.         │
```

## Socket paths

| Path | Direction |
|---|---|
| `/tmp/rtmm.in.sock` | Generator -> Matchmaker |
| `/tmp/rtmm.out.sock` | Matchmaker -> Validator |

## Configuration

Default parameters (in `src/matchmaking/config.rs`):

| Parameter | Default | Description |
|---|---|---|
| `skill_rating_range` | 2500 | Range [0, 2500] discretised into buckets |
| `bucket_width` | 50 | Rating span per bucket (50 buckets total) |
| `bucket_capacity` | 1000 | Max requests per bucket (overflow dropped) |
| `requests_per_matching` | 10 | Players per match |
| `expected_time_to_matching` | 3 min | Target wait time for resize policy |
| `delta_ceiling` | 5 | Max bucket expansion for matching |
| Worker count | 4 (hardcoded in `matchmaker.rs`) | Ground allocator threads |

## Running

```bash
# All processes (recommended)
./run-all.sh

# Individual (each in its own terminal, in order):
cargo run --release --bin validator
cargo run --release --bin matchmaker
cargo run --release --bin generator
```

## Testing

```bash
cargo test              # 96 tests (85 unit + 11 integration)
cargo test --lib        # unit tests only
cargo test --test '*'   # integration tests only
```
