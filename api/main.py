from fastapi import FastAPI

app = FastAPI(
    title="COVID-19 Analytics API",
    description="Data API for the National Public Health Analytics Consortium",
    version="1.0.0",
)


@app.get("/health")
def health_check():
    return {"status": "ok"}


# TODO: /api/v1/states/{state}/summary
# TODO: /api/v1/trends
# TODO: /api/v1/quality/status
