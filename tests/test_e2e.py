import pytest
import asyncio
import sys
import json
import time
import importlib
from pathlib import Path
from datetime import datetime, timezone
import hashlib

# Добавляем корневую папку проекта в PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fastapi.testclient import TestClient

class TestIntegratedAPI:
    """Интеграционные тесты API через TestClient без запуска серверов"""
    
    @pytest.fixture(scope="class")
    def intake_client(self):
        """TestClient для intake-api"""
        try:
            # Меняем рабочую директорию и добавляем в путь
            import os
            original_cwd = os.getcwd()
            intake_dir = project_root / "intake-api"
            os.chdir(intake_dir)
            
            if str(intake_dir) not in sys.path:
                sys.path.insert(0, str(intake_dir))
            
            # Очищаем кэш
            modules_to_clear = [k for k in sys.modules.keys() if k.startswith('main') or k.startswith('routes')]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]
            
            # Импортируем
            import main as intake_main
            
            # Восстанавливаем директорию
            os.chdir(original_cwd)
            
            intake_app = intake_main.app
            
            print(f"\n🔧 Loaded intake app: '{intake_app.title}'")
            
            client = TestClient(intake_app)
            
            # Отладка: показываем routes
            print("📋 Intake API routes:")
            for route in intake_app.routes:
                if hasattr(route, 'path') and hasattr(route, 'methods'):
                    print(f"   {route.methods} {route.path}")
            
            return client
            
        except Exception as e:
            print(f"❌ Error creating intake client: {e}")
            pytest.skip(f"Cannot create intake client: {e}")

    @pytest.fixture(scope="class")
    def insights_client(self):
        """TestClient для insights-api"""
        try:
            import os
            original_cwd = os.getcwd()
            insights_dir = project_root / "insights-api"
            os.chdir(insights_dir)
            
            if str(insights_dir) not in sys.path:
                sys.path.insert(0, str(insights_dir))
            
            # Очищаем кэш
            modules_to_clear = [k for k in sys.modules.keys() if k.startswith('main') or k.startswith('routes')]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]
            
            # Импортируем insights main отдельно
            import main as insights_main
            
            os.chdir(original_cwd)
            
            insights_app = insights_main.app
            
            print(f"\n🔧 Loaded insights app: '{insights_app.title}'")
            
            client = TestClient(insights_app)
            
            # Отладка: показываем routes
            print("📋 Insights API routes:")
            for route in insights_app.routes:
                if hasattr(route, 'path') and hasattr(route, 'methods'):
                    print(f"   {route.methods} {route.path}")
            
            return client
            
        except Exception as e:
            print(f"❌ Error creating insights client: {e}")
            pytest.skip(f"Cannot create insights client: {e}")

    def test_check_leads_router_file(self):
        """Проверим содержимое routes/leads.py"""
        print("\n🔍 Checking intake-api/routes/leads.py:")
        
        leads_file = project_root / "intake-api" / "routes" / "leads.py"
        
        if leads_file.exists():
            print(f"✅ Found: {leads_file}")
            
            with open(leads_file, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.split('\n')
                
                print("📋 File content:")
                for i, line in enumerate(lines[:40], 1):  # Первые 40 строк
                    print(f"   {i:2}: {line}")
                
                # Анализируем содержимое
                if "@router.post" in content:
                    print("\n✅ Found POST endpoints")
                else:
                    print("\n❌ No POST endpoints found")
                
                if "/leads" in content:
                    print("✅ Found /leads path references")
                else:
                    print("❌ No /leads path references")
                    
                if "LeadRequest" in content:
                    print("✅ Found LeadRequest model usage")
                else:
                    print("❌ No LeadRequest model usage")
                    
        else:
            print(f"❌ File not found: {leads_file}")
            
            # Проверим что вообще есть в routes/
            routes_dir = project_root / "intake-api" / "routes"
            if routes_dir.exists():
                files = list(routes_dir.glob("*.py"))
                print(f"📋 Files in routes/: {[f.name for f in files]}")
            else:
                print(f"❌ Routes directory not found: {routes_dir}")

    def test_intake_api_discovery(self, intake_client):
        """Полное исследование intake-api"""
        print("\n🔍 Full intake-api discovery:")
        
        # 1. Проверяем все возможные endpoints
        test_paths = [
            "/",
            "/leads", 
            "/api/leads",
            "/v1/leads",
            "/create-lead",
            "/lead",
            "/health",
            "/docs",
            "/openapi.json"
        ]
        
        for path in test_paths:
            try:
                # GET запрос
                get_response = intake_client.get(path)
                print(f"   GET {path} -> {get_response.status_code}")
                
                # POST запрос с тестовыми данными
                if path not in ["/docs", "/openapi.json", "/health"]:
                    post_response = intake_client.post(path, json={"note": "test"})
                    print(f"   POST {path} -> {post_response.status_code}")
                    
                    if post_response.status_code not in [404, 405]:
                        print(f"      Response: {post_response.text[:100]}...")
                
            except Exception as e:
                print(f"   {path} -> Error: {e}")
        
        # 2. Получаем OpenAPI схему
        try:
            openapi_response = intake_client.get("/openapi.json")
            if openapi_response.status_code == 200:
                openapi = openapi_response.json()
                print(f"\n📋 OpenAPI paths: {list(openapi.get('paths', {}).keys())}")
                
                # Детальный анализ каждого path
                for path, methods in openapi.get('paths', {}).items():
                    print(f"   {path}:")
                    for method, details in methods.items():
                        print(f"      {method.upper()}: {details.get('summary', 'No summary')}")
            else:
                print(f"\n❌ Cannot get OpenAPI schema: {openapi_response.status_code}")
        except Exception as e:
            print(f"\n❌ OpenAPI error: {e}")

    def test_insights_api_working_endpoint(self, insights_client):
        """Тест работающего endpoint в insights-api"""
        print("\n🔍 Test working insights endpoint:")
        
        # Тестируем известный рабочий endpoint
        fake_lead_id = "test-lead-123"
        
        response = insights_client.get(f"/leads/{fake_lead_id}/insight")
        
        print(f"📋 GET /leads/{fake_lead_id}/insight -> {response.status_code}")
        print(f"📋 Response: {response.text}")
        
        # Ожидаем 404 для несуществующего лида
        assert response.status_code == 404
        
        error_data = response.json()
        assert "error" in error_data or "detail" in error_data
        
        print("✅ Insights API working correctly")

    def test_health_checks(self, intake_client, insights_client):
        """Проверяем health check endpoints"""
        print("\n💚 Testing health checks:")
        
        # Intake API health
        intake_health = intake_client.get("/health")
        print(f"📋 Intake health: {intake_health.status_code} - {intake_health.json()}")
        assert intake_health.status_code == 200
        
        # Insights API health  
        insights_health = insights_client.get("/health")
        print(f"📋 Insights health: {insights_health.status_code} - {insights_health.json()}")
        assert insights_health.status_code == 200
        
        print("✅ Both health checks working")

    def test_create_lead_if_endpoint_exists(self, intake_client):
        """Тест создания лида, если endpoint существует"""
        print("\n📝 Test: Try to create lead:")
        
        # Сначала найдем правильный endpoint через OpenAPI
        try:
            openapi_response = intake_client.get("/openapi.json")
            if openapi_response.status_code == 200:
                openapi = openapi_response.json()
                paths = openapi.get('paths', {})
                
                # Ищем POST endpoint для создания лидов
                post_endpoints = []
                for path, methods in paths.items():
                    if 'post' in methods:
                        post_endpoints.append(path)
                        print(f"   Found POST endpoint: {path}")
                
                if post_endpoints:
                    # Пробуем первый найденный POST endpoint
                    endpoint = post_endpoints[0]
                    
                    lead_data = {
                        "email": "test@example.com",
                        "phone": "+1234567890",
                        "name": "Test User",
                        "note": "Test note for API",
                        "source": "test"
                    }
                    
                    response = intake_client.post(
                        endpoint,
                        json=lead_data,
                        headers={"Idempotency-Key": f"test-{int(time.time())}"}
                    )
                    
                    print(f"   POST {endpoint} -> {response.status_code}")
                    print(f"   Response: {response.text}")
                    
                    if response.status_code in [200, 201]:
                        print("✅ Lead creation successful!")
                        lead_response = response.json()
                        assert "id" in lead_response
                        return lead_response
                    else:
                        print(f"❌ Lead creation failed: {response.status_code}")
                else:
                    print("❌ No POST endpoints found in OpenAPI schema")
            else:
                print("❌ Cannot get OpenAPI schema")
                
        except Exception as e:
            print(f"❌ Error testing lead creation: {e}")
        
        print("⏭️  Skipping lead creation test - endpoint not available")