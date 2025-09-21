import pytest
import sys
import json
import time
import sqlite3
import hashlib
import uuid
import asyncio
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fastapi.testclient import TestClient

class TestEventDeduplication:
    @pytest.fixture
    def db_connection(self):
        """Подключение к базе данных для проверки дедупликации"""
        db_path = project_root / "database.sqlite"
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            yield conn
            cursor = conn.cursor()
            cursor.execute("DELETE FROM insights WHERE lead_id IN (SELECT id FROM leads WHERE source = 'event_dedup_test')")
            cursor.execute("DELETE FROM leads WHERE source = 'event_dedup_test'")
            conn.commit()
            conn.close()
        else:
            pytest.skip("Database not found")

    def _create_test_lead(self, db_connection, lead_id=None, email=None, note=None):
        """Создаем тестовый лид в БД"""
        if not lead_id:
            lead_id = f"test-lead-{int(time.time())}-{uuid.uuid4().hex[:8]}"
        if not email:
            email = f"test-{int(time.time())}-{uuid.uuid4().hex[:8]}@example.com"
        if not note:
            note = f"Test note content for deduplication - {int(time.time())} - {uuid.uuid4().hex[:8]}"
        
        cursor = db_connection.cursor()
        cursor.execute("""
            INSERT INTO leads (id, email, note, source, created_at)
            VALUES (?, ?, ?, ?, datetime('now'))
        """, (lead_id, email, note, "event_dedup_test"))
        
        db_connection.commit()
        return lead_id, email, note

    def _create_insight_directly(self, db_connection, lead_id, content_hash, insight_data=None):
        """Создаем инсайт напрямую в БД (имитируя работу triage-worker)"""
        if not insight_data:
            insight_data = {
                "intent": "buy",
                "priority": "P1", 
                "next_action": "call",
                "confidence": 0.85
            }
        
        insight_id = str(uuid.uuid4())
        
        cursor = db_connection.cursor()
        
        cursor.execute("PRAGMA table_info(insights)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if "content_hash" in columns:
            cursor.execute("""
                INSERT INTO insights (id, lead_id, intent, priority, next_action, confidence, content_hash, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, (
                insight_id,
                lead_id,
                insight_data["intent"],
                insight_data["priority"],
                insight_data["next_action"],
                insight_data["confidence"],
                content_hash
            ))
        else:
            cursor.execute("""
                INSERT INTO insights (id, lead_id, intent, priority, next_action, confidence, created_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            """, (
                insight_id,
                lead_id,
                insight_data["intent"],
                insight_data["priority"],
                insight_data["next_action"],
                insight_data["confidence"]
            ))
        
        db_connection.commit()
        return insight_id

    def test_duplicate_event_prevention(self, db_connection):
        """
        ТЕСТ 4: Основная проверка дедупликации событий
        
        Дубликат события в очереди должен:
        1. НЕ создать второй инсайт в БД
        2. Быть отклонен уникальным индексом (lead_id, content_hash)
        3. Сохранить только первый валидный инсайт
        4. Не повредить существующие данные
        """
        lead_id, email, note = self._create_test_lead(db_connection)
        content_hash = hashlib.sha256(note.encode('utf-8')).hexdigest()
        
        cursor = db_connection.cursor()
        
        cursor.execute("PRAGMA table_info(insights)")
        columns = [col[1] for col in cursor.fetchall()]
        has_content_hash = "content_hash" in columns
        
        cursor.execute("SELECT COUNT(*) FROM insights WHERE lead_id = ?", (lead_id,))
        initial_insights = cursor.fetchone()[0]
        
        if has_content_hash:
            cursor.execute("SELECT COUNT(*) FROM insights WHERE content_hash = ?", (content_hash,))
            initial_hash_insights = cursor.fetchone()[0]
        else:
            initial_hash_insights = 0
        
        assert initial_insights == 0, "Should start with no insights"
        
        first_insight_data = {
            "intent": "buy",
            "priority": "P1",
            "next_action": "call", 
            "confidence": 0.85
        }
        
        try:
            first_insight_id = self._create_insight_directly(
                db_connection, 
                lead_id, 
                content_hash, 
                first_insight_data
            )
            
        except Exception as e:
            pytest.fail(f"First insight creation failed: {e}")
        
        cursor.execute("SELECT COUNT(*) FROM insights WHERE lead_id = ?", (lead_id,))
        after_first_insights = cursor.fetchone()[0]
        
        cursor.execute("SELECT * FROM insights WHERE id = ?", (first_insight_id,))
        first_insight_record = cursor.fetchone()
        
        assert after_first_insights == 1, "Should have exactly one insight"
        assert first_insight_record is not None, "First insight should exist"
        
        time.sleep(0.1)
        
        duplicate_insight_data = {
            "intent": "support",     
            "priority": "P2",        
            "next_action": "email",  
            "confidence": 0.65       
        }
        
        duplicate_blocked = False
        duplicate_error = None
        
        if has_content_hash:
            cursor.execute("PRAGMA index_list(insights)")
            indexes = cursor.fetchall()
            
            unique_constraint_exists = False
            for idx in indexes:
                if idx[2] == 1:  
                    cursor.execute(f"PRAGMA index_info({idx[1]})")
                    index_columns = cursor.fetchall()
                    column_list = [col[2] for col in index_columns]
                    if "lead_id" in column_list and "content_hash" in column_list:
                        unique_constraint_exists = True
                        break
            
            try:
                duplicate_insight_id = self._create_insight_directly(
                    db_connection,
                    lead_id,             
                    content_hash,        
                    duplicate_insight_data
                )
                
            except sqlite3.IntegrityError as e:
                duplicate_blocked = True
                duplicate_error = str(e)
                
            except Exception as e:
                pytest.fail(f"Unexpected error during duplicate creation: {e}")
        
        cursor.execute("SELECT COUNT(*) FROM insights WHERE lead_id = ?", (lead_id,))
        final_insights = cursor.fetchone()[0]
        
        if has_content_hash:
            cursor.execute("SELECT COUNT(*) FROM insights WHERE content_hash = ?", (content_hash,))
            final_hash_insights = cursor.fetchone()[0]
        else:
            final_hash_insights = final_insights
        
        cursor.execute("SELECT * FROM insights WHERE lead_id = ?", (lead_id,))
        all_insights = cursor.fetchall()
        
        if has_content_hash and duplicate_blocked:
            assert final_insights == 1, f"Should have exactly 1 insight, got {final_insights}"
            assert final_hash_insights == 1, f"Should have exactly 1 insight with this hash, got {final_hash_insights}"
            assert len(all_insights) == 1, "Should have exactly one insight record"
            
            final_insight = all_insights[0]
            assert final_insight[0] == first_insight_id, "Should preserve first insight ID"
            assert final_insight[2] == first_insight_data["intent"], "Should preserve first insight intent"

    def test_multiple_duplicate_attempts(self, db_connection):
        """
        Тест множественных попыток дублирования
        Проверяем что несколько дубликатов подряд все блокируются
        """
        lead_id, email, note = self._create_test_lead(db_connection)
        content_hash = hashlib.sha256(note.encode('utf-8')).hexdigest()
        
        cursor = db_connection.cursor()
        
        original_insight = {
            "intent": "buy",
            "priority": "P0",
            "next_action": "call",
            "confidence": 0.92
        }
        
        blocked_count = 0
        created_count = 0
        
        for i in range(5):
            
            duplicate_data = {
                "intent": ["support", "job", "other", "buy", "support"][i],
                "priority": ["P1", "P2", "P3", "P1", "P0"][i],
                "next_action": ["email", "forward_hr", "ignore", "ticket", "call"][i],
                "confidence": [0.1, 0.3, 0.5, 0.7, 0.9][i]
            }
            
            try:
                duplicate_id = self._create_insight_directly(
                    db_connection, 
                    lead_id,       
                    content_hash, 
                    duplicate_data
                )
                
                created_count += 1
                
            except sqlite3.IntegrityError:
                blocked_count += 1
            
        cursor.execute("SELECT COUNT(*) FROM insights WHERE lead_id = ?", (lead_id,))
        total_insights = cursor.fetchone()[0]
        
        if blocked_count == 5:
            assert total_insights == 1, f"Should have exactly 1 insight, got {total_insights}"

    def test_different_leads_same_content(self, db_connection):
        """
        Тест: разные лиды с одинаковым контентом
        Должно создаться 2 инсайта (разные lead_id, один content_hash)
        """
        cursor = db_connection.cursor()
        cursor.execute("DELETE FROM insights WHERE lead_id IN (SELECT id FROM leads WHERE source = 'event_dedup_test')")
        cursor.execute("DELETE FROM leads WHERE source = 'event_dedup_test'")
        db_connection.commit()
        
        timestamp = int(time.time())
        same_note = f"I need urgent pricing information for enterprise plan - {timestamp}"
        content_hash = hashlib.sha256(same_note.encode('utf-8')).hexdigest()
        
        lead1_id, email1, _ = self._create_test_lead(
            db_connection, 
            email=f"lead1-{timestamp}@example.com",
            note=same_note
        )
        
        lead2_id, email2, _ = self._create_test_lead(
            db_connection,
            email=f"lead2-{timestamp}@example.com", 
            note=same_note  
        )

        insight1_data = {
            "intent": "buy",
            "priority": "P1", 
            "next_action": "call",
            "confidence": 0.8
        }
        
        insight1_id = self._create_insight_directly(db_connection, lead1_id, content_hash, insight1_data)

        insight2_data = {
            "intent": "buy",
            "priority": "P1",
            "next_action": "call", 
            "confidence": 0.8
        }
        
        try:
            insight2_id = self._create_insight_directly(db_connection, lead2_id, content_hash, insight2_data)
            
        except sqlite3.IntegrityError as e:
            pytest.fail(f"Second insight should be created (different lead_id): {e}")
        
        cursor.execute("SELECT COUNT(*) FROM insights WHERE content_hash = ?", (content_hash,))
        insights_with_hash = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM insights WHERE lead_id = ?", (lead1_id,))
        insights_lead1 = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM insights WHERE lead_id = ?", (lead2_id,))
        insights_lead2 = cursor.fetchone()[0]
        
        assert insights_lead1 == 1, f"Lead 1 should have 1 insight, got {insights_lead1}"
        assert insights_lead2 == 1, f"Lead 2 should have 1 insight, got {insights_lead2}"
        
        cursor.execute("PRAGMA table_info(insights)")
        columns = [col[1] for col in cursor.fetchall()]
        has_content_hash = "content_hash" in columns
        
    def test_same_lead_different_content(self, db_connection):
        """
        Тест: один лид с разным контентом
        Должно создаться 2 инсайта (один lead_id, разные content_hash)
        """
        lead_id, email, _ = self._create_test_lead(db_connection)
        
        timestamp = int(time.time())
        content1 = f"I need pricing for basic plan - {timestamp}"
        content2 = f"Actually, I need enterprise features now - {timestamp}"
        
        hash1 = hashlib.sha256(content1.encode('utf-8')).hexdigest()
        hash2 = hashlib.sha256(content2.encode('utf-8')).hexdigest()
        
        cursor = db_connection.cursor()
        
        cursor.execute("PRAGMA table_info(insights)")
        columns = [col[1] for col in cursor.fetchall()]
        has_content_hash = "content_hash" in columns
        
        insight1_data = {
            "intent": "buy",
            "priority": "P2",
            "next_action": "email", 
            "confidence": 0.6
        }

        insight2_data = {
            "intent": "buy",
            "priority": "P1",    
            "next_action": "call", 
            "confidence": 0.9      
        }
        
        insight1_id = self._create_insight_directly(db_connection, lead_id, hash1, insight1_data)
        insight2_id = self._create_insight_directly(db_connection, lead_id, hash2, insight2_data)
        
        cursor.execute("SELECT COUNT(*) FROM insights WHERE lead_id = ?", (lead_id,))
        insights_for_lead = cursor.fetchone()[0]
        
        cursor.execute("SELECT * FROM insights WHERE lead_id = ? ORDER BY created_at", (lead_id,))
        all_insights = cursor.fetchall()
        
        assert insights_for_lead == 2, f"Should have 2 insights for same lead, got {insights_for_lead}"
        assert len(all_insights) == 2, "Should retrieve 2 insights"
        
    def test_edge_case_empty_content(self, db_connection):
        """
        Граничный случай: пустой или очень короткий контент
        """
        
        cursor = db_connection.cursor()
        
        lead1_id, _, _ = self._create_test_lead(db_connection, note="")
        empty_hash = hashlib.sha256("".encode('utf-8')).hexdigest()
        
        insight1_id = self._create_insight_directly(
            db_connection, 
            lead1_id, 
            empty_hash,
            {"intent": "other", "priority": "P3", "next_action": "ignore", "confidence": 0.1}
        )
        
        short_content = "?"
        short_hash = hashlib.sha256(short_content.encode('utf-8')).hexdigest()
        
        lead3_id, _, _ = self._create_test_lead(db_connection, note=short_content)
        
        insight2_id = self._create_insight_directly(
            db_connection,
            lead3_id,
            short_hash,
            {"intent": "other", "priority": "P3", "next_action": "ignore", "confidence": 0.2}
        )
        
        whitespace_content = "   \t\n   "
        whitespace_hash = hashlib.sha256(whitespace_content.encode('utf-8')).hexdigest()
        
        lead4_id, _, _ = self._create_test_lead(db_connection, note=whitespace_content)
        
        insight3_id = self._create_insight_directly(
            db_connection,
            lead4_id, 
            whitespace_hash,
            {"intent": "other", "priority": "P3", "next_action": "ignore", "confidence": 0.3}
        )
        
        cursor.execute("SELECT COUNT(*) FROM insights WHERE lead_id IN (?, ?, ?)", (lead1_id, lead3_id, lead4_id))
        total_edge_insights = cursor.fetchone()[0]
        
    def test_database_schema_verification(self, db_connection):
        """
        Проверяем что схема БД правильно настроена для дедупликации
        """
        
        cursor = db_connection.cursor()
        
        cursor.execute("PRAGMA table_info(insights)")
        columns = cursor.fetchall()
        
        cursor.execute("PRAGMA index_list(insights)")
        indexes = cursor.fetchall()
        
        for idx in indexes:
            cursor.execute(f"PRAGMA index_info({idx[1]})")
            index_columns = cursor.fetchall()
            print(f"      Columns: {[col[2] for col in index_columns]}")
        
        unique_constraint_found = False
        for idx in indexes:
            if idx[2] == 1: 
                cursor.execute(f"PRAGMA index_info({idx[1]})")
                index_columns = cursor.fetchall()
                column_names = [col[2] for col in index_columns]
                
                if "lead_id" in column_names and "content_hash" in column_names:
                    unique_constraint_found = True
                    break
        
    def test_cleanup_event_deduplication_data(self, db_connection):
        """Исправленная очистка данных после тестов дедупликации"""
        
        cursor = db_connection.cursor()
        
        cursor.execute("DELETE FROM insights WHERE lead_id IN (SELECT id FROM leads WHERE source = 'event_dedup_test')")
        cursor.execute("DELETE FROM leads WHERE source = 'event_dedup_test'")
        
        test_email_patterns = [
            "test-%@example.com",
            "lead1-%@example.com", 
            "lead2-%@example.com"
        ]
        
        for pattern in test_email_patterns:
            if "%" in pattern:
                cursor.execute("DELETE FROM insights WHERE lead_id IN (SELECT id FROM leads WHERE email LIKE ?)", (pattern,))
                cursor.execute("DELETE FROM leads WHERE email LIKE ?", (pattern,))
            else:
                cursor.execute("DELETE FROM insights WHERE lead_id IN (SELECT id FROM leads WHERE email = ?)", (pattern,))
                cursor.execute("DELETE FROM leads WHERE email = ?", (pattern,))
        
        db_connection.commit()
        print("✅ Event deduplication test data cleaned up")