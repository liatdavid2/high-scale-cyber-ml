import os

def _context_text(matches: list[dict]) -> str:
    chunks = []
    for i, m in enumerate(matches, 1):
        chunks.append(
            f"[{i}] source={m.get('source')} id={m.get('id')} name={m.get('name')}\n"
            f"{m.get('text', '')}"
        )
    return "\n\n".join(chunks)

def answer_with_context(query: str, matches: list[dict]):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {
            "mode": "retrieval_only",
            "message": "OPENAI_API_KEY is not set; returning retrieved knowledge only."
        }

    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    model = os.getenv("OPENAI_MODEL", "gpt-5-mini")
    context = _context_text(matches)

    prompt = f"""You are a cybersecurity analyst.
Answer only from the retrieved context below.
If the context is insufficient, say so.
Be concise and operational.

Question:
{query}

Retrieved context:
{context}
"""

    response = client.responses.create(
        model=model,
        input=prompt,
    )
    return {
        "mode": "rag",
        "model": model,
        "text": response.output_text,
    }
