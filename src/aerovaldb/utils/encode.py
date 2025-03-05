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
    ls: list[str] = []
    prev = 0
    i = 0
    while i < len(string):
        if string[i] != "%":
            i += 1
            continue

        for k, v in encode_chars.items():
            if string[i : (i + 2)] == v:
                ls.append(string[prev:i] + k)
                i += 2
                prev = i
                break
    ls.append(string[prev:])

    return "".join(ls)
