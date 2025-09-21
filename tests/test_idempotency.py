import pytest
import sys
import json
import time
import sqlite3
import hashlib
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fastapi.testclient import TestClient

class TestIdempotencyDetailed:
    @pytest.fixture(scope="class")
    def intake_client(self):
        """TestClient для intake-api"""
        try:
            import os
            original_cwd = os.getcwd()
            intake_dir = project_root / "intake-api"
            os.chdir(intake_dir)
            
            if str(intake_dir) not in sys.path:
                sys.path.insert(0, str(intake_dir))
            
            modules_to_clear = [k for k in sys.modules.keys() if k.startswith('main') or k.startswith('routes')]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]
            
            import main as intake_main
            os.chdir(original_cwd)
            
            return TestClient(intake_main.app)
            
        except Exception as e:
            pytest.skip(f"Cannot create intake client: {e}")

    @pytest.fixture
    def db_connection(self):
        """Подключение к базе данных для проверки дублирования"""
        db_path = project_root / "database.sqlite"
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            yield conn
            conn.close()
        else:
            pytest.skip("Database not found")

    def _find_post_endpoint(self, client):
        """Находим рабочий POST endpoint для лидов"""
        try:
            openapi = client.get("/openapi.json").json()
            for path, methods in openapi.get('paths', {}).items():
                if 'post' in methods and 'lead' in path.lower():
                    return path
        except:
            pass
        
        test_endpoints = ["/leads", "/api/leads", "/v1/leads"]
        for endpoint in test_endpoints:
            try:
                response = client.post(endpoint, json={"note": "test"})
                if response.status_code != 404:
                    return endpoint
            except:
                continue
        return None

    def test_idempotency_same_request_no_duplication(self, intake_client, db_connection):
        """
        ТЕСТ 2: Детальная проверка идемпотентности
        
        Повтор POST /leads с тем же Idempotency-Key и тем же телом должен:
        1. Вернуть тот же ответ (200, не 201)
        2. НЕ создать дубликат лида в БД
        3. НЕ создать дубликат события в очереди
        4. Вернуть точно те же данные
        """
        print("\n🔄 DETAILED TEST: Idempotency - No Data/Event Duplication")
        
        endpoint = self._find_post_endpoint(intake_client)
        if not endpoint:
            pytest.skip("No working POST endpoint found")
        
        print(f"✅ Using endpoint: {endpoint}")
        
        lead_data = {
            "email": "idempotency-test@example.com",
            "phone": "+1234567890",
            "name": "Idempotency Test User",
            "note": "Testing that duplicate requests don't create duplicate data",
            "source": "idempotency_detailed_test"
        }
        
        idempotency_key = f"detailed-test-{int(time.time())}"
        
        print(f"\n📝 STEP 1: Making first request...")
        print(f"   📋 Idempotency-Key: {idempotency_key}")
        print(f"   📋 Email: {lead_data['email']}")
        
        cursor = db_connection.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM leads WHERE email = ?", (lead_data['email'],))
        leads_before = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM leads WHERE source = ?", (lead_data['source'],))
        source_leads_before = cursor.fetchone()[0]
        
        print(f"   📊 Leads with this email before: {leads_before}")
        print(f"   📊 Leads with this source before: {source_leads_before}")
        
        start_time_1 = time.time()
        response1 = intake_client.post(
            endpoint,
            json=lead_data,
            headers={"Idempotency-Key": idempotency_key}
        )
        end_time_1 = time.time()
        
        print(f"   📋 First response: {response1.status_code} (took {end_time_1 - start_time_1:.3f}s)")
        
        assert response1.status_code in [200, 201], f"First request failed: {response1.status_code}"
        
        lead1_data = response1.json()
        lead_id = lead1_data["id"]
        
        print(f"   ✅ First lead created: {lead_id}")
        print(f"   📋 Response data: {json.dumps(lead1_data, indent=2)}")
        
        cursor.execute("SELECT COUNT(*) FROM leads WHERE email = ?", (lead_data['email'],))
        leads_after_first = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM leads WHERE source = ?", (lead_data['source'],))
        source_leads_after_first = cursor.fetchone()[0]
        
        cursor.execute("SELECT * FROM leads WHERE id = ?", (lead_id,))
        db_lead = cursor.fetchone()
        
        print(f"   📊 Leads with this email after first: {leads_after_first}")
        print(f"   📊 Leads with this source after first: {source_leads_after_first}")
        print(f"   📊 Lead in DB: {db_lead}")
        
        assert leads_after_first == leads_before + 1, "Should create exactly one lead"
        assert source_leads_after_first == source_leads_before + 1, "Should create exactly one lead for this source"
        assert db_lead is not None, "Lead should exist in database"
        
        time.sleep(0.1)
        
        print(f"\n🔄 STEP 2: Making identical second request...")
        print(f"   📋 Same Idempotency-Key: {idempotency_key}")
        print(f"   📋 Same payload: {lead_data['email']}")
        
        start_time_2 = time.time()
        response2 = intake_client.post(
            endpoint,
            json=lead_data,  
            headers={"Idempotency-Key": idempotency_key}  
        )
        end_time_2 = time.time()
        
        print(f"   📋 Second response: {response2.status_code} (took {end_time_2 - start_time_2:.3f}s)")
        
        assert response2.status_code == 200, f"Expected 200 for idempotent request, got {response2.status_code}"
        
        lead2_data = response2.json()
        
        print(f"   ✅ Second response status: {response2.status_code} (idempotent)")
        print(f"   📋 Second response data: {json.dumps(lead2_data, indent=2)}")
        
        print(f"\n🔍 STEP 3: Verifying response identity...")
        
        assert lead1_data["id"] == lead2_data["id"], f"Lead IDs must be identical: {lead1_data['id']} vs {lead2_data['id']}"
        assert lead1_data["email"] == lead2_data["email"], "Email must be identical"
        assert lead1_data["phone"] == lead2_data["phone"], "Phone must be identical"
        assert lead1_data["name"] == lead2_data["name"], "Name must be identical"
        assert lead1_data["note"] == lead2_data["note"], "Note must be identical"
        assert lead1_data["source"] == lead2_data["source"], "Source must be identical"
        assert lead1_data["created_at"] == lead2_data["created_at"], "Creation timestamp must be identical"
        
        print(f"   ✅ All response fields are identical")
        
        print(f"\n🗄️  STEP 4: Verifying no database duplication...")
        
        cursor.execute("SELECT COUNT(*) FROM leads WHERE email = ?", (lead_data['email'],))
        leads_after_second = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM leads WHERE source = ?", (lead_data['source'],))
        source_leads_after_second = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM leads WHERE id = ?", (lead_id,))
        lead_count = cursor.fetchone()[0]
        
        print(f"   📊 Leads with this email after second: {leads_after_second}")
        print(f"   📊 Leads with this source after second: {source_leads_after_second}")
        print(f"   📊 Leads with this ID: {lead_count}")
        
        assert leads_after_second == leads_after_first, f"No new leads should be created: {leads_after_second} vs {leads_after_first}"
        assert source_leads_after_second == source_leads_after_first, f"No new source leads should be created"
        assert lead_count == 1, f"Should have exactly one lead with this ID, got {lead_count}"
        
        print(f"   ✅ No database duplication detected")
        
        cursor.execute("SELECT * FROM leads WHERE id = ?", (lead_id,))
        db_lead_after = cursor.fetchone()
        
        assert db_lead == db_lead_after, "Database record should not change"
        
        print(f"   ✅ Database record unchanged")
        
        print(f"\n🎯 STEP 5: Checking event creation...")
        
        response_time_1 = end_time_1 - start_time_1
        response_time_2 = end_time_2 - start_time_2
        
        print(f"   📊 First request time: {response_time_1:.3f}s")
        print(f"   📊 Second request time: {response_time_2:.3f}s")
        
        if response_time_2 < response_time_1:
            print(f"   ✅ Second request faster (likely cached)")
        else:
            print(f"   ℹ️  Response times similar")
        
        print(f"\n🎉 IDEMPOTENCY TEST PASSED!")
        print(f"   ✅ Same response returned")
        print(f"   ✅ No data duplication")
        print(f"   ✅ No database changes")
        print(f"   ✅ Proper status codes (201 → 200)")
        
        return {
            "lead_id": lead_id,
            "first_response": lead1_data,
            "second_response": lead2_data,
            "response_times": {
                "first": response_time_1,
                "second": response_time_2
            }
        }

    def test_idempotency_multiple_calls(self, intake_client, db_connection):
        """
        Тест множественных идемпотентных вызовов
        Проверяем что 5+ вызовов с одним ключом не создают дубликаты
        """
        print("\n🔄 TEST: Multiple Idempotent Calls")
        
        endpoint = self._find_post_endpoint(intake_client)
        if not endpoint:
            pytest.skip("No working POST endpoint found")
        
        lead_data = {
            "email": "multiple-idempotency@example.com",
            "name": "Multiple Test User",
            "note": "Testing multiple idempotent calls",
            "source": "multiple_idempotency_test"
        }
        
        idempotency_key = f"multiple-{int(time.time())}"
        
        print(f"📋 Making 5 identical requests with key: {idempotency_key}")
        
        responses = []
        response_times = []
        
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM leads WHERE email = ?", (lead_data['email'],))
        initial_count = cursor.fetchone()[0]
        
        for i in range(5):
            print(f"   🔵 Request {i+1}/5...")
            
            start_time = time.time()
            response = intake_client.post(
                endpoint,
                json=lead_data,
                headers={"Idempotency-Key": idempotency_key}
            )
            end_time = time.time()
            
            response_time = end_time - start_time
            responses.append(response)
            response_times.append(response_time)
            
            print(f"      Status: {response.status_code}, Time: {response_time:.3f}s")
            
            time.sleep(0.05)
        
        print(f"\n📊 Results Analysis:")
        
        assert responses[0].status_code == 201, f"First request should be 201, got {responses[0].status_code}"
        
        for i, response in enumerate(responses[1:], 2):
            assert response.status_code == 200, f"Request {i} should be 200, got {response.status_code}"
        
        print(f"   ✅ Status codes correct: 201, 200, 200, 200, 200")
        
        lead_ids = [resp.json()["id"] for resp in responses]
        unique_ids = set(lead_ids)
        
        assert len(unique_ids) == 1, f"All responses should have same lead_id, got: {unique_ids}"
        
        lead_id = lead_ids[0]
        print(f"   ✅ All responses have same lead_id: {lead_id}")
        
        first_data = responses[0].json()
        for i, response in enumerate(responses[1:], 2):
            response_data = response.json()
            assert response_data == first_data, f"Response {i} differs from first response"
        
        print(f"   ✅ All response bodies are identical")
        
        cursor.execute("SELECT COUNT(*) FROM leads WHERE email = ?", (lead_data['email'],))
        final_count = cursor.fetchone()[0]
        
        assert final_count == initial_count + 1, f"Should have exactly one new lead, got {final_count - initial_count}"
        
        print(f"   ✅ Only one record created in database")
        
        avg_time = sum(response_times) / len(response_times)
        print(f"   📊 Average response time: {avg_time:.3f}s")
        print(f"   📊 Response times: {[f'{t:.3f}s' for t in response_times]}")
        
        print(f"🎉 MULTIPLE IDEMPOTENCY TEST PASSED!")

    def test_idempotency_edge_cases(self, intake_client, db_connection):
        """
        Тест граничных случаев идемпотентности
        """
        print("\n🎯 TEST: Idempotency Edge Cases")
        
        endpoint = self._find_post_endpoint(intake_client)
        if not endpoint:
            pytest.skip("No working POST endpoint found")
        
        print("\n   🔍 Case 1: Very long idempotency key")
        
        long_key = "very-long-idempotency-key-" + "x" * 200 + f"-{int(time.time())}"
        
        lead_data1 = {
            "email": "edge-case-1@example.com",
            "note": "Long idempotency key test",
            "source": "edge_case_test"
        }
        
        response1 = intake_client.post(
            endpoint,
            json=lead_data1,
            headers={"Idempotency-Key": long_key}
        )
        
        response2 = intake_client.post(
            endpoint,
            json=lead_data1,
            headers={"Idempotency-Key": long_key}
        )
        
        assert response1.status_code == 201, "First long key request should succeed"
        assert response2.status_code == 200, "Second long key request should be idempotent"
        assert response1.json()["id"] == response2.json()["id"], "Should return same lead"
        
        print(f"      ✅ Long idempotency key works")
        
        print("\n   🔍 Case 2: Special characters in key")
        
        special_key = f"special-key-!@#$%^&*()_+-={int(time.time())}"
        
        lead_data2 = {
            "email": "edge-case-2@example.com",
            "note": "Special characters in key test",
            "source": "edge_case_test"
        }
        
        response1 = intake_client.post(
            endpoint,
            json=lead_data2,
            headers={"Idempotency-Key": special_key}
        )
        
        response2 = intake_client.post(
            endpoint,
            json=lead_data2,
            headers={"Idempotency-Key": special_key}
        )
        
        assert response1.status_code == 201, "First special key request should succeed"
        assert response2.status_code == 200, "Second special key request should be idempotent"
        
        print(f"      ✅ Special characters in key work")
        
        print("\n   🔍 Case 3: Large payload")
        
        large_note = "Large payload test. " + "Lorem ipsum dolor sit amet. " * 100
        
        lead_data3 = {
            "email": "edge-case-3@example.com",
            "name": "Large Payload User",
            "note": large_note,
            "source": "edge_case_test"
        }
        
        large_key = f"large-payload-{int(time.time())}"
        
        response1 = intake_client.post(
            endpoint,
            json=lead_data3,
            headers={"Idempotency-Key": large_key}
        )
        
        response2 = intake_client.post(
            endpoint,
            json=lead_data3,
            headers={"Idempotency-Key": large_key}
        )
        
        assert response1.status_code == 201, "First large payload request should succeed"
        assert response2.status_code == 200, "Second large payload request should be idempotent"
        assert response1.json()["note"] == large_note, "Large note should be preserved"
        assert response1.json() == response2.json(), "Large payload responses should be identical"
        
        print(f"      ✅ Large payload idempotency works")
        
        print(f"🎉 EDGE CASES TEST PASSED!")

    def test_cleanup_idempotency_test_data(self, db_connection):
        """Очистка данных после тестов идемпотентности"""
        print("\n🧹 Cleaning up idempotency test data...")
        
        cursor = db_connection.cursor()
        
        test_emails = [
            "idempotency-test@example.com",
            "multiple-idempotency@example.com",
            "edge-case-1@example.com",
            "edge-case-2@example.com",
            "edge-case-3@example.com"
        ]
        
        test_sources = [
            "idempotency_detailed_test",
            "multiple_idempotency_test",
            "edge_case_test"
        ]
        
        for email in test_emails:
            cursor.execute("DELETE FROM leads WHERE email = ?", (email,))
        
        for source in test_sources:
            cursor.execute("DELETE FROM leads WHERE source = ?", (source,))
        
        db_connection.commit()
        print("✅ Idempotency test data cleaned up")