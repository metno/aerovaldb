import aerovaldb

with aerovaldb.open("json_files:.") as db:
    db.acquire_lock()
    data = db.get_by_uuid("./file.json", default={"counter": 0})
    data["counter"] += 1
    db.put_by_uuid(data, "./file.json")
    db.release_lock()
