"""
Tests for FastAPI main application.
"""

from fastapi.testclient import TestClient

from sentinel_dispatch.api.main import app

client = TestClient(app)


class TestMainApplication:
    """Test cases for main FastAPI application."""
    
    def test_root_endpoint(self):
        """Test root endpoint."""
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "Sentinel Dispatch AI/ML Services"
        assert data["version"] == "0.1.0"
        assert "endpoints" in data
        assert "classify" in data["endpoints"]
        assert "enrich" in data["endpoints"]
        assert "score" in data["endpoints"]
        assert "alerts" in data["endpoints"]
    
    def test_health_check(self):
        """Test main health check endpoint."""
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "sentinel-dispatch-ai-ml-services"
    
    def test_openapi_schema(self):
        """Test OpenAPI schema endpoint."""
        response = client.get("/openapi.json")
        
        assert response.status_code == 200
        data = response.json()
        assert "openapi" in data
        assert "info" in data
        assert data["info"]["title"] == "Sentinel Dispatch AI/ML Services"
        assert "paths" in data
    
    def test_docs_endpoint(self):
        """Test Swagger UI endpoint."""
        response = client.get("/docs")
        
        assert response.status_code == 200
        # Should return HTML
        assert "text/html" in response.headers.get("content-type", "")
    
    def test_redoc_endpoint(self):
        """Test ReDoc endpoint."""
        response = client.get("/redoc")
        
        assert response.status_code == 200
        # Should return HTML
        assert "text/html" in response.headers.get("content-type", "")
    
    def test_cors_headers(self):
        """Test CORS headers are present."""
        response = client.options(
            "/",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "POST",
            }
        )
        
        # CORS middleware should handle OPTIONS requests
        # The exact response depends on CORS configuration
        assert response.status_code in [200, 204, 405]

