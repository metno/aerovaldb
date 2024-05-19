from fastapi import FastAPI, APIRouter

app = FastAPI()
router = APIRouter()


@app.get("/")
async def root():
    return {"message": "Hello World"}


async def func(var):
    return var


f = func
for i in ["/a/{name}", "/b/{hallo}", "/c/{whatever}"]:
    router.add_api_route(i, endpoint=f)

app.include_router(router)
