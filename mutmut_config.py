FILES_TO_SKIP = [
    "src/aerovaldb/routes.py",
    "src/aerovaldb/aerovaldb.py",
    "src/aerovaldb/generate_endpoints_from_config.py",
]


def pre_mutation(context):
    print(context.filename)
    if context.filename in FILES_TO_SKIP:
        context.skip = True
