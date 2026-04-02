"""PROCESS domain — clip captioning, frame extraction, raw video frames.

Modules:
    assets          @asset entry points (clip_captioning, clip_to_frame, raw_video_to_frame)
    helpers         Shared private helpers (clip window planning, ffprobe, temp file mgmt)
    captioning      clip_captioning MVP and routed implementations
    frame_extract   clip_to_frame MVP and routed implementations
    raw_frames      raw_video_to_frame implementation
    sensor          Process-stage sensors
"""
