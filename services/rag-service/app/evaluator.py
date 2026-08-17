import json
import os


def evaluate_generation(query: str, answer: str, matches: list[dict]):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set; generation evaluation requires OpenAI.")

    from openai import OpenAI

    client = OpenAI(api_key=api_key)
    model = os.getenv("OPENAI_EVAL_MODEL", os.getenv("OPENAI_MODEL", "gpt-5-mini"))

    context = "\n\n".join(
        f"[{i}] source={m.get('source')} id={m.get('id')} name={m.get('name')}\n{m.get('text', '')}"
        for i, m in enumerate(matches, 1)
    )

    prompt = f"""You are evaluating a cybersecurity RAG answer.
Score only these two dimensions from 0.0 to 1.0:
1. groundedness: how well the answer is supported by the retrieved context, with no unsupported claims.
2. relevance: how directly and usefully the answer addresses the user's question.

Return JSON only in this exact shape:
{{"groundedness": 0.0, "relevance": 0.0, "notes": "short explanation"}}

Question:
{query}

Answer:
{answer}

Retrieved context:
{context}
"""

    response = client.responses.create(model=model, input=prompt)
    text = (response.output_text or "").strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()

    try:
        result = json.loads(text)
    except Exception as exc:
        raise RuntimeError(f"Could not parse evaluation JSON: {text[:300]}") from exc

    usage = getattr(response, "usage", None)
    result["groundedness"] = max(0.0, min(1.0, float(result.get("groundedness", 0.0))))
    result["relevance"] = max(0.0, min(1.0, float(result.get("relevance", 0.0))))
    result["notes"] = str(result.get("notes", ""))[:1000]
    result["model"] = model
    result["input_tokens"] = int(getattr(usage, "input_tokens", 0) or 0) if usage else 0
    result["output_tokens"] = int(getattr(usage, "output_tokens", 0) or 0) if usage else 0
    return result
