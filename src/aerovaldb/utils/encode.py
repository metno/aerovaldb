def encode_str(string: str, *, encode_chars: dict[str, str]):
    ls: list[str] = []
    prev = 0
    i = 0
    while i < len(string):
        if string[i] in encode_chars:
            ls.append(string[prev:i] + encode_chars.get(string[i]))  # type: ignore
            prev = i + 1
        i += 1

    ls.append(string[prev:])

    return "".join(ls)


def decode_str(string: str, *, encode_chars: dict[str, str]):
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
