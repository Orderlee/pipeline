"""Gemini prompt templates reused inside the data pipeline."""

IMAGE_PROMPT = """
You are a video surveillance training expert.

Based on a single frame extracted from CCTV footage,
you must generate a high-difficulty security training question
that can be used by security professionals or AI models for training purposes.

Please generate the question in structured JSON format according to the guidelines below.

---

[Objectives]
- The question must have a clear, unambiguous answer and high educational value.
- The answer should be inferable based solely on visual evidence from the single frame.
- Various question types are acceptable, but the output format must strictly follow the JSON structure.
- Do not force importance onto routine or uneventful scenes; plain "normal_activity" does not need to be treated as a notable situation.

---

| Question Type | Description | Key Evaluation Focus | Example Question |
| --- | --- | --- | --- |
| Multiple Choice | A 4-option question asking to infer person/object/action/situation | Visual identification, situational reasoning | "What is this person doing?" |
| True/False (Binary Judgment) | A declarative question to answer with Yes or No | Factual reasoning, time/location recognition | "Was this scene captured in a restricted area?" |
| Short Answer | A brief descriptive answer about situational judgment or appropriate security action | Response decision-making, inference | "What should the security officer do in this situation?" |
| Action Classification | Classify or select the action of a specific person in the frame | Pose recognition, interaction analysis | "What is the person in the red shirt doing?" |
| Object Existence | Asks whether a specific object is present in the frame | Object detection ability | "Is there a firearm visible in the frame?" |
| Spatial Reasoning | Determine the relative position or direction of an object or person | Spatial awareness | "Is the person located on the left side of the frame?" |
| Anomaly Detection | Identify whether the scene is abnormal, and explain why | Contextual reasoning, norm awareness | "Does this scene appear to be abnormal?" |
| Crime Type Matching | Match an abnormal scene to a specific crime category | Scene-to-concept classification | "If this scene is abnormal, what type of crime does it indicate?" |
| Temporal Reasoning | Infer temporal context from visual clues | Interpretation of time-related cues | "Was this scene captured during nighttime hours?" |
| Security Response | Decide what would be the most appropriate action in the given situation | Decision-making, tactical planning | "What is the most appropriate security response in this situation?" |

---

[Output JSON Structure]
{
  "question_type": "Type of question (e.g., Multiple Choice, True/False, Short Answer, Action Classification, etc.)",
  "question": "The question text",
  "options": ["Option A", "Option B", "Option C", "Option D"],
  "answer": "Correct answer",
  "rationale": "A clear explanation of the visual/logical reasoning behind the correct answer",
  "intended_skill": "The skill intended to be evaluated",
  "difficulty": "Either 'Intermediate' or 'Advanced'"
}
""".strip()


VIDEO_PROMPT = """
You are a video surveillance training expert.

Based on a CCTV video clip,
you must generate a high-difficulty security training question
that can be used by security professionals or AI models for training purposes.

The question must strictly follow the structured JSON format below.

---

[Objectives]
- The question must be clear, unambiguous, and have high educational value.
- The correct answer must be inferable solely from the provided video clip using visual and contextual evidence.
- Various question types are acceptable, but the output must strictly follow the JSON structure below.
- Do not force importance onto routine or uneventful scenes; plain "normal_activity" does not need to be treated as a notable situation.

---

| Question Type | Description | Key Evaluation Focus | Example Question |
| --- | --- | --- | --- |
| Multiple Choice | A 4-option question inferring person/object/action/situation | Visual identification, situational reasoning | "What is this person doing?" |
| True/False (Binary Judgment) | A declarative question to answer with Yes or No | Factual reasoning, time/location recognition | "Was this scene captured in a restricted area?" |
| Short Answer | A brief descriptive answer about situational judgment or appropriate security action | Decision-making, inference | "What should the security officer do in this situation?" |
| Action Classification | Classify or select the action of a specific person in the video | Pose recognition, interaction analysis | "What is the person in the red shirt doing?" |
| Object Existence | Asks whether a specific object is present in the video | Object detection ability | "Is there a firearm visible in this scene?" |
| Spatial Reasoning | Determine the relative position or direction of an object or person | Spatial awareness | "Is the person located on the left side of the frame?" |
| Anomaly Detection | Identify whether the scene is abnormal and explain why | Contextual reasoning, norm awareness | "Does this scene appear to be abnormal?" |
| Crime Type Matching | Match an abnormal scene to a specific crime category | Scene-to-concept classification | "If this scene is abnormal, what type of crime does it indicate?" |
| Temporal Reasoning | Infer temporal context from visual clues | Time interpretation | "Was this scene captured during nighttime hours?" |
| Security Response | Decide the most appropriate action in the given situation | Decision-making, tactical planning | "What is the most appropriate security response in this situation?" |
| Abnormality Scoring | Evaluate the abnormality level of the entire video on a 0-1 scale | Abnormality detection and quantification | "Based on the video, what is the abnormality score (0-1)?" |

---

[Output JSON Structure]
{
  "question_type": "Type of question",
  "question": "The question text",
  "options": ["Option A", "Option B", "Option C", "Option D"],
  "answer": "Correct answer",
  "rationale": "A clear explanation of the visual/logical reasoning behind the correct answer",
  "intended_skill": "The skill intended to be evaluated",
  "difficulty": "Either 'Intermediate' or 'Advanced'"
}
""".strip()


VIDEO_EVENT_PROMPT = """
You are an expert video event annotator for surveillance and safety monitoring.

Watch the entire video carefully and identify all distinct events or incidents.
For each event, provide the precise time segment and a descriptive caption.

Return your response as a JSON array. Each element must have exactly these fields:

- "category": A short English category label (e.g. "fire", "smoke", "fall", "intrusion", "fight", "vehicle_accident", "loitering", "vandalism", "abandoned_object")
- "duration": Duration of the event in seconds (number)
- "timestamp": [start_sec, end_sec] — start and end time in seconds from the beginning of the video
- "ko_caption": A concise Korean description of the event
- "en_caption": A concise English description of the event

Rules:
- Timestamps must be non-negative and end_sec > start_sec.
- Routine or uneventful "normal_activity" does not need to be annotated; only include notable or security-relevant events.
- If no notable event is found, return an empty array: []
- Do NOT wrap the JSON in markdown code fences.
- Sort events by start time ascending.

Example output:
[
  {
    "category": "smoke",
    "duration": 3.5,
    "timestamp": [12.0, 15.5],
    "ko_caption": "건물 좌측에서 연기가 발생하여 점차 확산됨",
    "en_caption": "Smoke emerges from the left side of the building and gradually spreads"
  }
]
""".strip()


VIDEO_EVENT_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "category": {"type": "string"},
            "duration": {"type": "number"},
            "timestamp": {
                "type": "array",
                "items": {"type": "number"},
                "min_items": 2,
                "max_items": 2,
            },
            "ko_caption": {"type": "string"},
            "en_caption": {"type": "string"},
        },
        "required": ["category", "duration", "timestamp", "ko_caption", "en_caption"],
    },
}
