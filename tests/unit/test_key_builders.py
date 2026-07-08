"""Tests for vlm_pipeline.lib.key_builders — MinIO object key generation."""

from __future__ import annotations

from vlm_pipeline.lib.key_builders import (
    build_gemini_label_key,
    build_image_caption_key,
    build_image_classification_key,
    build_pseudo_bbox_key,
    build_processed_clip_image_key,
    build_processed_clip_key,
    build_raw_video_image_key,
    build_sam3_detection_key,
    build_sam3_segmentation_key,
    build_video_classification_key,
    build_yolo_label_key,
)


class TestBuildProcessedClipKey:
    def test_video_with_timestamps(self):
        key = build_processed_clip_key(
            "folder/video.mp4",
            event_index=0,
            clip_start_sec=10.5,
            clip_end_sec=20.3,
            media_type="video",
        )
        assert key == "folder/clips/video_00010500_00020300.mp4"

    def test_image_without_timestamps(self):
        key = build_processed_clip_key(
            "folder/photo.jpg",
            event_index=2,
            clip_start_sec=None,
            clip_end_sec=None,
            media_type="image",
        )
        assert key == "folder/clips/photo_e002.jpg"

    def test_bare_filename(self):
        key = build_processed_clip_key(
            "video.mp4",
            event_index=0,
            clip_start_sec=1.0,
            clip_end_sec=5.0,
            media_type="video",
        )
        assert key == "clips/video_00001000_00005000.mp4"


class TestBuildProcessedClipImageKey:
    def test_under_clips_parent(self):
        key = build_processed_clip_image_key("folder/clips/video_001.mp4", 3)
        assert key == "folder/image/video_001_00000003.jpg"

    def test_non_clips_parent(self):
        key = build_processed_clip_image_key("folder/video.mp4", 1)
        assert key == "folder/image/video_00000001.jpg"


class TestBuildImageCaptionKey:
    def test_under_image_parent(self):
        key = build_image_caption_key("folder/image/test_00000001.jpg")
        assert key == "folder/image_captions/test_00000001.json"

    def test_non_image_parent(self):
        key = build_image_caption_key("folder/test.jpg")
        assert key == "folder/image_captions/test.json"

    def test_bare_filename(self):
        key = build_image_caption_key("test.jpg")
        assert key == "image_captions/test.json"


class TestBuildYoloLabelKey:
    def test_under_image_parent(self):
        key = build_yolo_label_key("folder/image/test.jpg")
        assert key == "folder/detections/test.json"

    def test_non_image_parent(self):
        key = build_yolo_label_key("folder/test.jpg")
        assert key == "folder/detections/test.json"


class TestBuildImageClassificationKey:
    def test_under_image_parent(self):
        key = build_image_classification_key("folder/image/test.jpg")
        assert key == "folder/image_classifications/test.json"

    def test_bare(self):
        key = build_image_classification_key("test.jpg")
        assert key == "image_classifications/test.json"


class TestBuildVideoClassificationKey:
    def test_with_parent(self):
        key = build_video_classification_key("folder/video.mp4")
        assert key == "folder/video_classifications/video.json"

    def test_bare(self):
        key = build_video_classification_key("video.mp4")
        assert key == "video_classifications/video.json"


class TestBuildGeminiLabelKey:
    def test_with_parent(self):
        key = build_gemini_label_key("folder/video.mp4")
        assert key == "folder/events/video.json"

    def test_bare(self):
        key = build_gemini_label_key("video.mp4")
        assert key == "events/video.json"


class TestBuildRawVideoImageKey:
    def test_with_parent(self):
        key = build_raw_video_image_key("folder/video.mp4", 5)
        assert key == "folder/image/video_00000005.jpg"

    def test_already_image_parent(self):
        key = build_raw_video_image_key("folder/image/video.mp4", 1)
        assert key == "folder/image/video_00000001.jpg"


class TestBuildSam3SegmentationKey:
    def test_under_image_parent(self):
        key = build_sam3_segmentation_key("folder/image/test.jpg")
        assert key == "folder/sam3_segmentations/test.json"

    def test_bare(self):
        key = build_sam3_segmentation_key("test.jpg")
        assert key == "sam3_segmentations/test.json"


class TestBuildPseudoBboxKey:
    """C-1: write-once SAM3 원본 스냅샷 키. LS 리뷰가 덮어쓰는 라이브 키와 반드시 달라야 함."""

    def test_under_image_parent(self):
        key = build_pseudo_bbox_key("folder/image/test.jpg")
        assert key == "folder/sam3_segmentations/test.pseudo.json"

    def test_under_frames_parent(self):
        key = build_pseudo_bbox_key("folder/frames/test.jpg")
        assert key == "folder/frames/sam3_segmentations/test.pseudo.json"

    def test_bare(self):
        key = build_pseudo_bbox_key("test.jpg")
        assert key == "sam3_segmentations/test.pseudo.json"

    def test_differs_from_live_detection_key(self):
        image_key = "folder/image/test.jpg"
        assert build_pseudo_bbox_key(image_key) != build_sam3_detection_key(image_key)
        assert build_pseudo_bbox_key(image_key) == build_sam3_detection_key(image_key).replace(".json", ".pseudo.json")
