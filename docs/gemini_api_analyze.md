# Gemini API Auto-Labeling Pipeline Guide

> Baseline: no audio · default resolution · GS E&C 38 files (155,957 seconds, average 68.4 minutes)
> References: [https://ai.google.dev/gemini-api/docs/tokens](https://ai.google.dev/gemini-api/docs/tokens), [https://ai.google.dev/gemini-api/docs/video-understanding](https://ai.google.dev/gemini-api/docs/video-understanding)

---

## 1. Token Formula

```
tokens = duration_sec × 258

200k boundary = 200,000 / 258 = 775 seconds = 12.9 minutes
```

- Regardless of the original FPS (10–15), Gemini internally samples at a **fixed 1 FPS** (configurable — see FPS strategy below)
- 1 FPS × 258 tokens/frame = 258 tokens/sec
- 258 tokens/frame is a fixed value regardless of resolution (Gemini internal normalization)

---

## 2. Pricing Structure


|             | Flash        | Pro            |
| ----------- | ------------ | -------------- |
| Input ≤200k | $0.30/M      | $1.25/M        |
| Input >200k | $0.30/M (same) | $2.50/M (2×)  |
| Output      | $2.50/M      | $10.00–15.00/M |


> **M = 1 million tokens**
> Pro price doubles for any single request exceeding 200k tokens.
> Even with the same total tokens, cost varies depending on how the requests are split.

---

## 3. Cost Per Unit


| Unit            | Tokens      | Flash     | Pro ≤200k | Pro >200k |
| ------------- | --------- | --------- | --------- | --------- |
| 1 second            | 258       | $0.000077 | $0.000323 | $0.000645 |
| 1 minute            | 15,480    | $0.0046   | $0.0194   | $0.0387   |
| 1 hour           | 928,800   | $0.279    | $1.161    | $2.322    |
| Average 1 file (68.4 min) | 1,058,832 | $0.319    | —         | $2.652    |


> **1 hour Pro >200k**: 928,800 / 1,000,000 × $2.50 = $2.322
> (Differs from the $2.50 unit rate because 1 hour is not exactly 1 million tokens)
>
> **Average 1 file Pro ≤200k "—"**: 68.4 min = 1,058,832 tokens → always in the >200k tier

---

## 4. Cost by Scenario (GS E&C 38 files)


| Scenario           | Unit price            | Requests | Calculation                                 | Cost                    |
| -------------- | ------------- | ---- | ----------------------------------- | --------------------- |
| Flash 60-min (current) | $0.30/M       | 78   | 40.24M × $0.30                      | **$12.1 (₩17,900)**   |
| Pro 60-min (current)   | $2.50/M       | 78   | 40.24M × $2.50                      | **$100.7 (₩148,800)** |
| Pro 10-min chunk     | $1.25/M       | 260  | 40.24M × $1.25                      | **$50.3 (₩74,300)**   |
| 2-Pass Hybrid  | $0.30+$1.25/M | 253  | (40.24M × $0.30) + (26.47M × $1.25) | **$45.2 (₩66,800)**   |


```
Common:          total tokens = 155,957 sec × 258 = 40.24M tokens
2-Pass stage 2:  155,957 × 25/38 × 258  = 26.47M tokens (25 files)
```

### Why Pro 10-min chunks are cheaper

```
Sending as a whole: 1 file = 516,000 tokens > 200k → $2.50/M applies
Splitting into 10-min chunks: 1 chunk = 154,800 tokens < 200k → $1.25/M applies

Total tokens are the same; only the unit price changes: $2.50 → $1.25 (half)
→ $100.7 → $50.3
```

---

## 5. Processing Flow for a 1-Hour Video (Pro 10-min chunks)

```
Source video (60 min, ~525 MB)
  ↓
[Step 1] Preview transcoding (if >450 MB)
  Source (~525 MB) → preview (~150 MB, audio removed)
  Purpose: 4× upload speed improvement, audio token removal
  Source file is preserved

  ↓
[Step 2] Chunk splitting (with 60-second overlap)
  chunk_01: 0:00 – 11:00
  chunk_02: 9:00 – 20:00
  chunk_03: 19:00 – 30:00
  chunk_04: 29:00 – 40:00
  chunk_05: 39:00 – 50:00
  chunk_06: 49:00 – 60:00

  ↓
[Step 3] Gemini API calls (concurrent parallel)
  Each chunk → event timestamps returned (relative to chunk start)
  e.g. {"timestamp": [330, 375], "category": "vehicle_movement"}

  ↓
[Step 4] Offset correction (required)
  chunk_04 start = 1,800 seconds (30:00)
  330 sec → 1,800 + 330 = 2,130 sec (original 35:30)

  ↓
[Step 5] Deduplication (overlap region)
  Adjacent chunks with timestamp difference < 60 sec + same category → merge

  ↓
[Step 6] Clip extraction (from source)
  No buffer (unnecessary for YOLO purposes)
  Cut directly from source using timestamps

  ↓
[Step 7] Frame extraction → YOLO
  clips × f(d) frames extracted → YOLO object detection
```

---

## 6. Operational Optimization

### Chunk Strategy

```
Flash: 30-min chunks
  → No cost change, minimizes API overhead

Pro:   10-min chunks
  600 sec × 258 = 154,800 tokens < 200k → $1.25/M maintained
  → $100.7 → $50.3 (50% savings)

Overlap: 60 seconds required
  660 sec × 258 = 170,280 tokens < 200k ✓
  Prevents boundary event loss; ~10% cost increase (negligible)
```

### FPS Strategy

```
Default: 1 FPS (sufficient for construction site events lasting 5+ seconds)
         Configurable via Gemini API parameter
Exception: 2–3 FPS (when sub-3-second event detection is needed)
           → 2–3× cost; apply selectively to suspected segments only
```

### Maximizing Concurrency

```
max_concurrent = RPM / 60 × average_response_time_sec

Flash: 1,000 RPM, avg 9 sec → theoretical max 150, recommended 20–30
Pro:   150 RPM,   avg 18 sec → theoretical max 45,  recommended 10–15

For 260 requests with 10-min chunks:
  concurrent=3  → ~22 min
  concurrent=10 → ~7 min

On 429 (Too Many Requests) errors: reduce concurrency or apply exponential backoff
```

---

## 7. Processing Time


| Stage            | Flash    | Pro (10-min chunks) | Notes              |
| ------------- | -------- | ------------ | --------------- |
| Preview transcoding | ~10 min     | ~10 min         | Model-independent, 3 parallel      |
| Gemini API    | ~8 min      | ~22 min         | At concurrent=3 |
| Clip extraction         | ~1.5 min    | ~1.5 min        | Extracted from source         |
| YOLO          | ~0.3 min    | ~0.3 min        | batch=4         |
| **Total**        | **~20 min** | **~34 min**     |                 |


> Gemini calls start as soon as preview transcoding completes → stages overlap, so actual wall time is shorter than the sum

---

## 8. Storage

```
Source 38 files:   22.74 GB (100%)
Preview 30 files:   ~5 GB   (~22%)    Transcoding output
Event clips:        ~2–5% of source   Extracted from source
Frame images:       ~3–4% of source   JPEG input for YOLO
Chunk files:        ~1–2 GB           Temporary, deleted after processing
─────────────────────────────────────────────────────────────
Maximum required space (peak):    ~130–150% of source
Space after processing (final):   ~10% of source (estimated)
```

### Image Count Formula

```
clips  = V × n          V: number of videos / n: events per video (estimated)
images = clips × f(d)   d: clip length (seconds)

f(d): d < 10s    → 3 frames
      10 – 30s   → 5 frames
      30 – 120s  → 8 frames
      120s+      → 12 frames
```

---

## 9. Resources


| Resource        | Bottleneck      | Management notes                    |
| ---------- | ------- | ------------------------- |
| Gemini API | 🔴 Primary bottleneck | Check RPM limits, maximize concurrency |
| Network       | 🟡 Secondary bottleneck  | Larger chunks increase upload time          |
| CPU        | 🟡 Secondary bottleneck  | 3 parallel transcoding is currently optimal           |
| GPU (YOLO) | 🟢 Headroom   | Verify cuda device setting         |
| Disk        | 🟢 Headroom   | Reserve 150% of source as free space         |


---

## 10. Data Quality


| Item        | Detail                                        |
| --------- | ----------------------------------------- |
| Timestamp accuracy | ±1 second (structural limit of 1 FPS)                        |
| Boundary events    | 60-second overlap + post-deduplication processing                       |
| Clip extraction source  | Source (not preview; preserves YOLO quality)               |
| Buffer        | Unnecessary for YOLO purposes                             |
| Review priority   | Flag clips from chunk boundaries (`from_chunk_boundary`) for priority review |


---

## Selection Criteria

```
Cost first     → Flash 30-min chunks    $12.1/batch
Quality first  → Pro 10-min chunks      $50.3/batch
Optimal balance → 2-Pass Hybrid         $45.2/batch
```
