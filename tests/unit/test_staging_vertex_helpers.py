from __future__ import annotations

from vlm_pipeline.lib.staging_vertex import (
    merge_overlapping_events,
    offset_gemini_events,
    parse_event_frame_image_caption_response,
    plan_overlapping_video_chunks,
)


def test_plan_overlapping_video_chunks_for_exact_one_hour() -> None:
    chunks = plan_overlapping_video_chunks(3600, window_sec=660, stride_sec=600)

    assert [(chunk.start_sec, chunk.end_sec) for chunk in chunks] == [
        (0.0, 660.0),
        (600.0, 1260.0),
        (1200.0, 1860.0),
        (1800.0, 2460.0),
        (2400.0, 3060.0),
        (3000.0, 3600.0),
    ]


def test_merge_overlapping_events_deduplicates_same_category_after_offset() -> None:
    chunk_a = [
        {
            "category": "smoke",
            "duration": 20.0,
            "timestamp": [580.0, 620.0],
            "ko_caption": "앞 구간 연기",
            "en_caption": "smoke near overlap",
        }
    ]
    chunk_b = [
        {
            "category": "smoke",
            "duration": 20.0,
            "timestamp": [0.0, 30.0],
            "ko_caption": "더 긴 뒤 구간 연기 설명",
            "en_caption": "longer smoke description",
        }
    ]

    merged = merge_overlapping_events(
        offset_gemini_events(chunk_a, offset_sec=0.0, chunk_end_sec=660.0)
        + offset_gemini_events(chunk_b, offset_sec=600.0, chunk_end_sec=1260.0)
    )

    assert merged == [
        {
            "category": "smoke",
            "duration": 50.0,
            "timestamp": [580.0, 630.0],
            "ko_caption": "더 긴 뒤 구간 연기 설명",
            "en_caption": "longer smoke description",
        }
    ]


def test_parse_event_frame_image_caption_response_handles_irrelevant_frame() -> None:
    is_relevant, caption_text = parse_event_frame_image_caption_response(
        '{"is_relevant": false, "caption": null}'
    )

    assert is_relevant is False
    assert caption_text is None
