"""Compatibility CLI for Gemini video analysis.

This module keeps the newer shared Gemini integration, but exposes a
folder-first workflow compatible with the standalone `video.py` script
that was used for anomaly-event extraction.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from vlm_pipeline.lib.gemini import (
    GeminiAnalyzer,
    extract_clean_json_text,
    list_video_files,
)

DEFAULT_VIDEO_DIR = "archive"
DEFAULT_OUTPUT_DIR = "archive/test"
DEFAULT_RECURSIVE = False
DEFAULT_MODEL_NAME = "gemini-2.5-pro"
DEFAULT_VIDEO_PROMPT = """
이 비디오를 분석해서 비정상적이거나 특별한 이벤트가 발생하는 모든 구간을 찾아주세요.
결과는 반드시 아래와 같은 JSON 형식의 리스트로만 응답해야 합니다. 발생한 이벤트를 category에 작성해주세요.
다른 설명은 붙이지 마세요. duration에 해당 걸리는 시간을 작성해주시고요. 시간은 초 단위로 해주세요.
각 구간은 timestamp에 [시작시간, 종료시간] 초 단위로 작성해주세요. 해당 구간에 대한 ko_caption, en_caption 도 추가해주세요.
만약 이상 상황이 없다면 빈 리스트 [] 를 반환해주세요.
[
  {
    "category": "smoking",
    "duration": 30,
    "timestamp": [4, 34],
    "ko_caption": ["흡연으로 보이는 행동이 일정 시간 지속된다."],
    "en_caption": ["A person appears to be smoking for an extended period."]
  }
]
""".strip()


def _load_prompt(prompt: str | None, prompt_file: str | None) -> str | None:
    if prompt and prompt.strip():
        return prompt.strip()
    if prompt_file and prompt_file.strip():
        return Path(prompt_file).read_text(encoding="utf-8").strip()
    return DEFAULT_VIDEO_PROMPT


def _json_name_for_video(video_path: Path) -> str:
    return f"{video_path.stem}.json"


def _save_response_to_path(response_text: str, output_json_path: Path) -> str:
    cleaned = extract_clean_json_text(response_text)
    output_json_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        output_json_path.write_text(cleaned, encoding="utf-8")
        return str(output_json_path)

    output_json_path.write_text(
        json.dumps(parsed, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return str(output_json_path)


def _folder_output_path(video_path: Path, input_root: Path, output_root: Path) -> Path:
    relative_parent = video_path.parent.relative_to(input_root)
    return output_root / relative_parent / _json_name_for_video(video_path)


def _single_file_output_path(video_path: Path, output_root: Path) -> Path:
    return output_root / _json_name_for_video(video_path)


def _process_folder_with_logging(
    folder: Path,
    *,
    analyzer: GeminiAnalyzer,
    recursive: bool,
    prompt: str,
    mime_type: str | None,
    output_dir: Path,
) -> int:
    videos = list_video_files(str(folder), recursive=recursive)
    if not videos:
        print(f"[INFO] 영상 파일이 없습니다: {folder}")
        return 0

    print(f"[INFO] 총 {len(videos)}개 영상 처리 시작 (recursive={recursive})")

    success = 0
    failed = 0
    outputs: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []

    for idx, video_path in enumerate(videos, start=1):
        print(f"\n=== ({idx}/{len(videos)}) 처리 중 ===")
        print(f"영상: {video_path}")

        try:
            response_text = analyzer.analyze_video(
                video_path,
                prompt=prompt,
                mime_type=mime_type,
            )
            json_path = _save_response_to_path(
                response_text,
                _folder_output_path(Path(video_path), folder, output_dir),
            )
            print(f"[OK] 저장 완료: {json_path}")
            success += 1
            outputs.append({"video_path": video_path, "json_path": json_path})
        except FileNotFoundError:
            message = f"파일을 찾을 수 없습니다: {video_path}"
            print(f"[ERROR] {message}")
            failed += 1
            errors.append({"video_path": video_path, "error": message})
        except PermissionError:
            message = f"권한이 없어 읽을 수 없습니다: {video_path}"
            print(f"[ERROR] {message}")
            failed += 1
            errors.append({"video_path": video_path, "error": message})
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            print(f"[ERROR] 처리 중 예외 발생: {message}")
            failed += 1
            errors.append({"video_path": video_path, "error": message})

    print(f"\n[DONE] 완료: 성공 {success} / 실패 {failed} (총 {len(videos)})")
    print(
        json.dumps(
            {
                "folder": str(folder),
                "recursive": recursive,
                "total": len(videos),
                "success": success,
                "failed": failed,
                "outputs": outputs,
                "errors": errors,
                "model_name": analyzer.model_name,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if failed == 0 else 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze a video or folder with Gemini.",
    )
    parser.add_argument(
        "input_path",
        nargs="?",
        default=DEFAULT_VIDEO_DIR,
        help="Video file path or directory path. Defaults to the archive folder workflow.",
    )
    parser.add_argument("--recursive", action="store_true", help="Recursively scan a directory.")
    parser.add_argument(
        "--model-name",
        default=DEFAULT_MODEL_NAME,
        help="Vertex AI model name.",
    )
    parser.add_argument("--project", default=None, help="Override GEMINI_PROJECT.")
    parser.add_argument("--location", default=None, help="Override GEMINI_LOCATION.")
    parser.add_argument("--credentials-path", default=None, help="Credential JSON path.")
    parser.add_argument("--prompt", default=None, help="Inline prompt override.")
    parser.add_argument("--prompt-file", default=None, help="Read prompt text from file.")
    parser.add_argument("--mime-type", default=None, help="Explicit MIME type for video input.")
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where Gemini JSON outputs are written.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    target = Path(args.input_path).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    prompt = _load_prompt(args.prompt, args.prompt_file)
    recursive = bool(args.recursive or DEFAULT_RECURSIVE)

    analyzer = GeminiAnalyzer(
        model_name=args.model_name,
        project=args.project,
        location=args.location,
        credentials_path=args.credentials_path,
    )

    if target.is_dir():
        return _process_folder_with_logging(
            target,
            analyzer=analyzer,
            recursive=recursive,
            prompt=prompt,
            mime_type=args.mime_type,
            output_dir=output_dir,
        )

    if not target.is_file():
        raise FileNotFoundError(f"Input path not found: {target}")

    response_text = analyzer.analyze_video(
        str(target),
        prompt=prompt,
        mime_type=args.mime_type,
    )
    json_path = _save_response_to_path(
        response_text,
        _single_file_output_path(target, output_dir),
    )
    print(
        json.dumps(
            {
                "video_path": str(target),
                "json_path": json_path,
                "model_name": analyzer.model_name,
                "prompt_type": "anomaly_event_segments",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
