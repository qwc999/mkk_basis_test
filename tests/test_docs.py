from uuid import uuid4

import httpx

from app.config import Settings
from app.main import create_app


async def test_local_swagger_exposes_schema_with_api_key_authorization():
    settings = Settings(
        database_url="postgresql+asyncpg://user:password@db/payments",
        api_key="test-only-secret",
        enable_docs=True,
        _env_file=None,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(settings)), base_url="http://test"
    ) as client:
        docs = await client.get("/docs")
        assert docs.status_code == 200
        assert "/openapi.json" in docs.text
        response = await client.get("/openapi.json")
        assert response.status_code == 200
        schema = response.json()
        assert schema["components"]["securitySchemes"]["APIKeyHeader"] == {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
        }
        for path, method in (
            ("/api/v1/payments", "post"),
            ("/api/v1/payments/{payment_id}", "get"),
        ):
            assert schema["paths"][path][method]["security"] == [{"APIKeyHeader": []}]
        assert "test-only-secret" not in response.text + docs.text
        assert (await client.get("/redoc")).status_code == 404
        assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 401
        assert (await client.post("/api/v1/payments", json={})).status_code == 401
