from functools import cache

import tiktoken
from langchain_text_splitters import RecursiveCharacterTextSplitter


@cache
def get_encoder():
    return tiktoken.get_encoding("cl100k_base")


def chunk_text(
    text: str,
    target_chunk_size: int = 4000,
    overlap_tokens: int = 100,
) -> list[str]:
    """
    Chunk text into overlapping chunks based on word boundaries using langchain_text_splitters.

    Args:
        text: The text to chunk
        target_chunk_size: Target number of tokens per chunk (7800-8000)
        overlap_tokens: Number of tokens to overlap between chunks (100-200)
        encoding_name: The tiktoken encoding to use

    Returns:
        List of text chunks with word-based boundaries and token overlap
    """
    if not text.strip():
        return []

    # Get the tiktoken encoder for token counting
    encoder = get_encoder()

    # Create RecursiveCharacterTextSplitter with token-based chunking
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=target_chunk_size,
        chunk_overlap=overlap_tokens,
        length_function=lambda x: len(encoder.encode(x)),
        separators=[
            "[PAGE BREAK]",  # Double newlines (paragraphs)
            "\n",  # Single newlines
            ". ",  # Sentence endings
            "! ",  # Exclamation endings
            "? ",  # Question endings
            "; ",  # Semicolons
            ", ",  # Commas
            " ",  # Spaces
            "",  # Character level (fallback)
        ],
        is_separator_regex=False,
    )

    # Split the text into chunks
    chunks = text_splitter.split_text(text)

    return chunks
