from openai import AsyncOpenAI


async def embed_text(text_list: list[str]) -> list[list[float]]:
    client = AsyncOpenAI()
    response = await client.embeddings.create(
        input=text_list, model="text-embedding-3-small", dimensions=256
    )
    return [i.embedding for i in response.data]
