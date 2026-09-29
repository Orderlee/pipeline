from __future__ import annotations

from vlm_pipeline.lib.sam3_compare import compare_prompt_boxes, parse_yolo_coco_payload, summarize_benchmark_rows


def test_parse_yolo_coco_payload_groups_boxes_by_class() -> None:
    payload = {
        "annotations": [
            {"category_id": 1, "bbox": [10, 20, 30, 40]},
            {"category_id": 2, "bbox": [100, 120, 50, 60]},
        ],
        "categories": [
            {"id": 1, "name": "fire"},
            {"id": 2, "name": "smoke"},
        ],
    }

    parsed = parse_yolo_coco_payload(payload)

    assert parsed == {
        "fire": [[10.0, 20.0, 40.0, 60.0]],
        "smoke": [[100.0, 120.0, 150.0, 180.0]],
    }


def test_compare_prompt_boxes_and_summary_metrics() -> None:
    row = compare_prompt_boxes(
        image_id="image-1",
        prompt_class="fire",
        yolo_boxes=[[10.0, 20.0, 40.0, 60.0]],
        sam_detections=[
            {
                "prompt_class": "fire",
                "mask_bbox": [10.0, 20.0, 40.0, 60.0],
                "model_box": [11.0, 21.0, 39.0, 59.0],
            }
        ],
        benchmark_id="bench-1",
        yolo_labels_key="unit/detections/frame_0001.json",
        sam3_labels_key="unit/sam3_segmentations/frame_0001.json",
    )

    summary = summarize_benchmark_rows(
        [row],
        benchmark_id="bench-1",
        total_images=1,
        sam3_total_latency_ms=[25.0],
        yolo_latency_ms=[12.0],
        gpu_memory_peak_gb=3.25,
    )

    assert row["matched_pair_count"] == 1
    assert row["matched_iou_mean"] == 1.0
    assert row["yolo_to_sam_coverage"] == 1.0
    assert row["sam_to_yolo_coverage"] == 1.0
    assert summary["matched_iou_mean"] == 1.0
    assert summary["sam3_avg_latency_ms"] == 25.0
    assert summary["yolo_avg_latency_ms"] == 12.0
    assert summary["gpu_memory_peak_gb"] == 3.25
