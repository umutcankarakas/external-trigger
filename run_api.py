"""Entry point for the FastAPI management API."""
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "api.app:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
    )
