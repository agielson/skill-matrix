
import pytest
import sys
import os
from unittest.mock import MagicMock, patch, Mock

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import app, get_user_data, insert_user_data, connect_db

# Импорт psycopg2 для тестов
try:
    import psycopg2
except ImportError:
    class MockPsycopg2:
        class errors:
            class UniqueViolation(Exception):
                pass
    psycopg2 = MockPsycopg2()


@pytest.fixture
def client():
    app.config['TESTING'] = True
    app.config['WTF_CSRF_ENABLED'] = False
    with app.test_client() as client:
        with app.app_context():
            yield client


class TestRegistrationModule:
    
    # Тест 1: Успешная регистрация
    @patch('app.insert_user_data')
    @patch('app.get_user_data')
    def test_register_success(self, mock_get_user, mock_insert_user, client):
        """
        № теста: 1 - Успешная регистрация
        """
        mock_get_user.return_value = None
        # ✅ ИСПРАВЛЕНО: возвращаем кортеж (True, None)
        mock_insert_user.return_value = (True, None)
        
        response = client.post('/register', data={
            'username': 'new_user',
            'employee_id': 'EMP001',
            'password': 'secure123'
        })
        
        assert response.status_code == 302
        assert '/?registered=True' in response.location
        mock_get_user.assert_called_once_with('new_user')
        mock_insert_user.assert_called_once_with('new_user', 'EMP001', 'secure123')
    
    # Тест 2: Существующий пользователь
    @patch('app.get_user_data')
    def test_register_existing_username(self, mock_get_user, client):
        """
        № теста: 2 - Попытка регистрации с существующим username
        """
        mock_get_user.return_value = {
            'username': 'existing_user',
            'employee_id': 'EMP001',
            'password_hash': 'hashed_pass'
        }
        
        response = client.post('/register', data={
            'username': 'existing_user',
            'employee_id': 'EMP002',
            'password': 'pass123'
        })
        
        assert response.status_code == 200
        assert 'Пользователь с таким именем уже существует' in response.get_data(as_text=True)
    
    # Тест 3: Пустые поля
    def test_register_empty_fields(self, client):
        """
        № теста: 3 - Проверка пустых полей
        """
        response = client.post('/register', data={
            'username': '',
            'employee_id': '',
            'password': ''
        })
        
        assert response.status_code == 200
        assert 'Все поля должны быть заполнены' in response.get_data(as_text=True)
    
    # Тест 4: Частично заполненные поля
    def test_register_partial_fields(self, client):
        """
        № теста: 4 - Отсутствует employee_id
        """
        response = client.post('/register', data={
            'username': 'user1',
            'employee_id': '',
            'password': 'pass'
        })
        
        assert response.status_code == 200
        assert 'Все поля должны быть заполнены' in response.get_data(as_text=True)
    
    # Тест 5: Дубликат employee_id
    @patch('app.get_user_data')
    @patch('app.insert_user_data')
    def test_register_duplicate_employee(self, mock_insert_user, mock_get_user, client):
        """
        № теста: 5 - Обработка дубликата employee_id
        """
        mock_get_user.return_value = None
        # ✅ ИСПРАВЛЕНО: возвращаем кортеж (False, error_message)
        mock_insert_user.return_value = (False, "Пользователь уже существует или идентификатор сотрудника уже зарегистрирован")
        
        response = client.post('/register', data={
            'username': 'user2',
            'employee_id': 'DUPLICATE_ID',
            'password': 'pass'
        })
        
        assert response.status_code == 200
        assert 'Пользователь с таким именем или идентификатором уже существует' in response.get_data(as_text=True)
    
    # Тест 6: Ошибка базы данных
    @patch('app.get_user_data')
    @patch('app.insert_user_data')
    def test_register_database_error(self, mock_insert_user, mock_get_user, client):
        """
        № теста: 6 - Ошибка подключения к БД
        """
        mock_get_user.return_value = None
        # ✅ ИСПРАВЛЕНО: возвращаем кортеж (False, error_message)
        mock_insert_user.return_value = (False, "Connection failed")
        
        response = client.post('/register', data={
            'username': 'user3',
            'employee_id': 'EMP003',
            'password': 'pass'
        })
        
        assert response.status_code == 200
        response_text = response.get_data(as_text=True)
        assert 'Ошибка при регистрации' in response_text
    
    # Тест 7: GET запрос
    def test_register_page_get(self, client):
        """
        № теста: 7 - Проверка отображения страницы
        """
        response = client.get('/register')
        
        assert response.status_code == 200
        response_text = response.get_data(as_text=True)
        assert 'Регистрация пользователя' in response_text
        assert 'username' in response_text
        assert 'employee_id' in response_text
        assert 'password' in response_text
    
    # Тест 8: Сообщение об успехе
    def test_register_success_message(self, client):
        """
        № теста: 8 - Сообщение после успешной регистрации
        """
        response = client.get('/register?registered=True')
        
        assert response.status_code == 200
        assert 'Регистрация прошла успешно' in response.get_data(as_text=True)


class TestInsertUserData:
    
    # Тест 9: Успешная вставка
    @patch('app.connect_db')
    def test_insert_user_success(self, mock_connect_db):
        """
        № теста: 9 - Успешная вставка пользователя
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect_db.return_value = mock_conn
        
        # ✅ ИСПРАВЛЕНО: функция возвращает кортеж
        success, error = insert_user_data('test_user', 'TEST001', 'test123')
        
        assert success is True
        assert error is None
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
    
    # Тест 10: Дубликат пользователя
    @patch('app.connect_db')
    def test_insert_user_duplicate(self, mock_connect_db):
        """
        Тест: Попытка вставки дубликата
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Создаем mock исключения UniqueViolation
        mock_cursor.execute.side_effect = psycopg2.errors.UniqueViolation()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect_db.return_value = mock_conn
        
        success, error = insert_user_data('duplicate_user', 'EMP999', 'pass')
        
        assert success is False
        assert "already exists" in error.lower()
    
    # Тест 11: Общая ошибка базы данных
    @patch('app.connect_db')
    def test_insert_user_general_error(self, mock_connect_db):
        """
        Тест: Общая ошибка БД
        """
        mock_connect_db.side_effect = Exception("Connection failed")
        
        success, error = insert_user_data('user', 'ID123', 'pass')
        
        assert success is False
        assert error == "Connection failed"


class TestGetUserData:
    
    # Тест 12: Успешное получение данных
    @patch('app.connect_db')
    def test_get_user_success(self, mock_connect_db):
        """
        Тест: Успешное получение данных пользователя
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ('hashed_pass', 'existing_user', 'EMP001')
        mock_conn.cursor.return_value = mock_cursor
        mock_connect_db.return_value = mock_conn
        
        result = get_user_data('existing_user')
        
        assert result is not None
        assert result['username'] == 'existing_user'
        assert result['employee_id'] == 'EMP001'
        assert result['password_hash'] == 'hashed_pass'
    
    # Тест 13: Пользователь не найден
    @patch('app.connect_db')
    def test_get_user_not_found(self, mock_connect_db):
        """
        Тест: Пользователь отсутствует
        """
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value = mock_cursor
        mock_connect_db.return_value = mock_conn
        
        result = get_user_data('nonexistent_user')
        
        assert result is None


if __name__ == "__main__":
    # Запуск тестов
    pytest.main([__file__, '-v', '-s'])