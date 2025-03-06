def encode_str(string: str, *, encode_chars: dict[str, str]) -> str:
    """Encodes a string by replacing characters by en encoded
    character sequence.

    :param string: String to be encoded.
    :param encode_chars: Mapping from character to replacement string
    sequence in encoded string.

    :return: Encoded string.
    """
    mapping = str.maketrans(encode_chars)
    return string.translate(mapping)


def decode_str(string: str, *, encode_chars: dict[str, str]) -> str:
    """Decodes a string previously encoded using encode_str.

    :param string: String to be decoded.
    :param encode_chars: Mapping from character to replacement string
    used during the original encode operation.
    :return: Decoded string.
    """
    for k, v in encode_chars.items():
        string = string.replace(v, k)
    return string
