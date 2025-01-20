import torch
from torch import Tensor

MAX_TOKENS = 512
"""How many tokens we can feed to BERT at a time"""

ROBERTA_SPACE_CHAR = "Ġ"
"""
Character indicating space in RoBERTa tokens. When RoBERTa tokenizes
"very traumatized," it'll output something `["very", "Ġtraumat", "ized"]` 
"""


def bert_sentence_embeddings(tokens: list[str], tokenizer, model) -> list[Tensor]:
    """Get embeddings for the given tokens (originally from a sentence)"""
    # The indices of the tokens in the vocabulary
    token_inds = tokenizer.convert_tokens_to_ids(tokens)

    # segments_ids tells you which sentence each token belongs to.
    # Since we only process one "sentence" at a time, all the IDs are 1
    segment_ids = [1] * len(tokens)

    # gradient calculation is disabled
    with torch.no_grad():
        outputs = model(torch.tensor([token_inds]), torch.tensor([segment_ids]))
        hidden_states = outputs[2]
    # concatenate the tensors for all layers
    # use "stack" to create new dimension in tensor
    token_embeddings = torch.stack(hidden_states, dim=0)
    # remove dimension 1, the "batches"
    token_embeddings = torch.squeeze(token_embeddings, dim=1)
    # swap dimensions 0 and 1 so we can loop over tokens
    token_embeddings = token_embeddings.permute(1, 0, 2)

    # intialized list to store embeddings
    token_vecs_sum = []
    # "token_embeddings" is a [Y x 12 x 768] tensor where Y is the number of tokens in the sentence
    # loop over tokens in sentence
    for token in token_embeddings:  # "token" is a [12 x 768] tensor
        # sum the vectors from the last four layers
        sum_vec = torch.sum(token[-4:], dim=0)
        token_vecs_sum.append(sum_vec)
    return token_vecs_sum


def bert_process(
    text: str, keywords: list[str], tokenizer, model
) -> tuple[list[str], list[tuple[int, str, Tensor]]]:
    """
    Returns a 2-tuple containing:
    1. The tokens
    2. A list. For each keyword found in the text, it contains:
        1. The start of the word in the list of tokens
        2. The full word
        3. Its embedding
    """
    if all(kw not in text for kw in keywords):
        return [], []

    tokens: list[str] = tokenizer.tokenize(text)

    # (text, start_ind, end_ind) for every word of interest in the sentence
    words = []

    # CAUTION: The token-smooshing bit is specific to roberta-base and would
    # need to be changed for BERTweet, which uses @@ instead
    curr_start = 0
    curr_word = tokens[0]
    for i in range(1, len(tokens) + 1):
        if i == len(tokens) or tokens[i].startswith(ROBERTA_SPACE_CHAR):
            if any(kw in curr_word for kw in keywords):
                words.append((curr_word, curr_start, i + 1))
            if i < len(tokens):
                curr_start = i
                curr_word = tokens[i].removeprefix(ROBERTA_SPACE_CHAR)
        else:
            curr_word += tokens[i]

    if len(tokens) < MAX_TOKENS:
        embeddings = bert_sentence_embeddings(tokens, tokenizer, model)
        out = []
        for word, start, end in words:
            embedding = torch.mean(torch.stack(embeddings[start:end]), dim=0)
            out.append((start, word, embedding))
        return tokens, out

    out = []
    for word, start, end in words:
        if len(tokens) - end < MAX_TOKENS / 2:
            sent_start = len(tokens) - MAX_TOKENS
        else:
            sent_start = max(0, end - int(MAX_TOKENS / 2))
        smol_sent = tokens[sent_start : sent_start + MAX_TOKENS]
        embeddings = bert_sentence_embeddings(smol_sent, tokenizer, model)
        embedding = torch.mean(
            torch.stack(embeddings[start - sent_start : end - sent_start]), dim=0
        )
        out.append((start, word, embedding))
    return tokens, out
