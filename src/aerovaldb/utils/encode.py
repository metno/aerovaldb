class EncodedStr(str):
    pass


class DecodedStr(str):
    pass


def encode_str(string: str, *, encode_chars: dict[str, str]) -> EncodedStr:
    """Encodes a string by replacing characters by en encoded
    character sequence.

    :param string: String to be encoded.
    :param encode_chars: Mapping from character to replacement string
    sequence in encoded string.

    :return: Encoded string.
    """
    if isinstance(string, EncodedStr):
        raise TypeError(f"String '{string}' is already encoded.")

    mapping = str.maketrans(encode_chars)
    return EncodedStr(string.translate(mapping))


def decode_str(string: str, *, encode_chars: dict[str, str]) -> DecodedStr:
    """Decodes a string previously encoded using encode_str.

    :param string: String to be decoded.
    :param encode_chars: Mapping from character to replacement string
    used during the original encode operation.
    :return: Decoded string.
    """
    if isinstance(string, DecodedStr):
        raise TypeError(f"String '{string}' is already decoded.")

    for k, v in encode_chars.items():
        string = string.replace(v, k)
    return DecodedStr(string)
