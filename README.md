# Assignment 3 – Spark Structured Streaming with checkpointing

**Name:** Ankona Mukherjee
**Registration No.:** 202400560

## Task
Write a Spark streaming program that streams data from a data source of my choice, and apply
appropriate methods for fault tolerance with checkpointing. Also: a small program to check duplicacy.

## Design
| Part | Choice |
|---|---|
| Source | New JSON-lines files appearing in `input/` (Spark file source) |
| Processing | `dropDuplicates(["event_id"])` with a 1-hour watermark, then running count and total amount per category |
| Sink | Console (`complete` output mode) |
| Fault tolerance | `checkpointLocation = checkpoint/` |

### How fault tolerance works
Spark writes the processed source offsets, the aggregation state and the de-duplication state to
`checkpoint/` after every micro-batch. If the job crashes or is stopped, restarting it with the same
checkpoint directory resumes from the last committed batch: files already processed are not read again,
and the totals continue from where they stopped. The watermark lets Spark discard old de-duplication
state, so memory stays bounded while duplicate events that are re-sent are still ignored.

## Files
| File | Purpose |
|---|---|
| `generate_events.py` | Simulates incoming data: writes a batch file into `input/` (about 15% of events are repeats) |
| `stream_job.py` | The streaming job (`--once` processes what is new and stops) |
| `check_duplicates.py` | Small program that checks duplicacy in `input/` |

## Run
```bash
pip install pyspark
python generate_events.py 20          # run a few times to create batches
python check_duplicates.py            # shows how many duplicate events exist
python stream_job.py                  # streams continuously; Ctrl+C to stop
python generate_events.py 20          # add more data while it runs, or after a restart
python stream_job.py                  # resumes from checkpoint/
```

## Result (tested)
Three batches (60 events, 9 duplicates, so 51 unique) gave 8 + 11 + 7 + 15 + 10 = **51** events across
the categories: duplicates were dropped. After adding two more batches and restarting the job, it
continued at batch 2, read only the new files, and the totals became 14 + 18 + 13 + 22 + 18 = **85**
unique events (100 sent, 15 duplicates): nothing lost and nothing counted twice.
