def get_embeddings_one_word(word):
    """
    Embeds a single word using the SentenceTransformers library.
    Args:
        word (str): The word to embed.
    Returns:
        dict: A dictionary with the word as key and the embeddings as value.
    """
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer("all-MiniLM-L6-v2")

    embeddings = model.encode(word)
    embeddings = embeddings.tolist()

    return {word: embeddings}
