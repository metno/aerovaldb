FILES_TO_SKIP = [
    "src/aerovaldb/routes.py",
    "src/aerovaldb/aerovaldb.py",
    "src/aerovaldb/generate_endpoints_from_config.py",
]


def pre_mutation(context):
    print(f"{context.filename}\n")
    line = context.current_source_line.strip()
    if line.startswith("logger."):
        context.skip = True
    if context.filename in FILES_TO_SKIP:
        context.skip = True
