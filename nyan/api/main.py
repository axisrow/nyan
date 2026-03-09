from fastapi import FastAPI

from nyan.api.routers import channels, clusters, documents, pipeline

app = FastAPI(
    title="nyan API",
    description="REST API для агрегатора новостей nyan",
    version="1.0.0",
)

app.include_router(clusters.router)
app.include_router(documents.router)
app.include_router(channels.router)
app.include_router(pipeline.router)


@app.get("/")
def root() -> dict:
    return {"status": "ok", "docs": "/docs"}
