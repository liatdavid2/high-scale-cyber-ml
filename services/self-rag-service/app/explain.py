import json
import os

FEATURE_LABELS = {
    "dur": "duration",
    "rate": "event rate",
    "total_bytes": "total bytes",
    "total_packets": "total packets",
    "byte_ratio_src_dst": "source/destination byte ratio",
    "packet_ratio_src_dst": "source/destination packet ratio",
    "bytes_per_packet": "bytes per packet",
}


def _relative_diff(a, b):
    a = float(a)
    b = float(b)
    denom = max(abs(a), abs(b), 1e-9)
    return abs(a - b) / denom


def explain_cases(query_features, matches):
    explained = []

    for rank, match in enumerate(matches, 1):
        diffs = []

        for key, label in FEATURE_LABELS.items():
            q = query_features.get(key, 0.0)
            m = match.get("features", {}).get(key, 0.0)

            diffs.append(
                (key, label, _relative_diff(q, m), q, m)
            )

        diffs.sort(key=lambda x: x[2])
        closest = diffs[:3]
        furthest = diffs[-2:]

        reasons = [
            f"{label}: query={q:.4g}, case={m:.4g}, difference={diff*100:.2f}%"
            for _, label, diff, q, m in closest
        ]

        caveats = [
            f"{label}: query={q:.4g}, case={m:.4g}, difference={diff*100:.2f}%"
            for _, label, diff, q, m in reversed(furthest)
            if diff > 0.20
        ]

        explained.append({
            "rank": rank,
            "row_index": match["row_index"],
            "similarity": match["similarity"],
            "label": match.get("label"),
            "attack_cat": match.get("attack_cat"),
            "why_similar": reasons,
            "differences": caveats,
        })

    return explained


def llm_self_check(query_features, matches, deterministic_explanation):
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        return {
            "mode": "disabled",
            "message": "OPENAI_API_KEY not set. Deterministic explanation only.",
        }

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        model = os.getenv("OPENAI_MODEL", "gpt-5-mini")

        prompt = f"""
You are evaluating a cybersecurity similar-case retrieval result.

The query and retrieved cases come from UNSW-NB15.
IMPORTANT:
- label and attack_cat were NOT used for retrieval.
- Similarity was calculated only from traffic features.
- Do not claim two cases are identical.
- Judge whether the deterministic explanation is supported by the numeric features.

Query features:
{json.dumps(query_features, indent=2)}

Retrieved matches:
{json.dumps(matches, indent=2)}

Deterministic explanation:
{json.dumps(deterministic_explanation, indent=2)}

Return compact JSON only:
{{
  "grounded": true,
  "retrieval_quality": 0.0,
  "explanation_quality": 0.0,
  "summary": "short explanation of whether these are genuinely similar cases"
}}

Scores must be between 0 and 1.
"""

        response = client.responses.create(
            model=model,
            input=prompt,
        )

        text = response.output_text.strip()

        try:
            parsed = json.loads(text)
        except Exception:
            parsed = {"raw": text}

        usage = getattr(response, "usage", None)

        return {
            "mode": "llm_self_check",
            "model": model,
            "result": parsed,
            "input_tokens": getattr(usage, "input_tokens", None) if usage else None,
            "output_tokens": getattr(usage, "output_tokens", None) if usage else None,
        }

    except Exception as exc:
        return {
            "mode": "error",
            "message": str(exc),
        }
