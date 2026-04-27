import time
import pytest
import bcrypt
from unittest.mock import patch, MagicMock

@pytest.fixture
def client():
    from app import app
    app.config['TESTING'] = True
    app.config['WTF_CSRF_ENABLED'] = False
    with app.test_client() as client:
        with app.app_context():
            yield client

class TestPerformanceTesting:
    """Тестирование производительности"""
    
    @patch('app.insert_user_data')
    @patch('app.get_user_data')
    def test_registration_performance(self, mock_get_user, mock_insert_user, client):
        """Проверка времени выполнения регистрации"""
        mock_get_user.return_value = None
        mock_insert_user.return_value = (True, None)
        
        start_time = time.time()
        response = client.post('/register', data={
            'username': 'perf_user',
            'employee_id': 'PERF001',
            'password': 'SecurePass123'
        })
        end_time = time.time()
        
        response_time = (end_time - start_time) * 1000
        print(f"Время регистрации: {response_time:.2f} мс")
        
        assert response.status_code == 302
        assert response_time < 1000
    
    @patch('app.get_user_data')
    def test_login_performance(self, mock_get_user, client):
        """Проверка времени выполнения входа"""
        password_hash = bcrypt.hashpw(b'testpass', bcrypt.gensalt()).decode('utf-8')
        
        mock_get_user.return_value = {
            'username': 'test_user',
            'employee_id': 'EMP001',
            'password_hash': password_hash
        }
        
        start_time = time.time()
        response = client.post('/', data={
            'username': 'test_user',
            'password': 'testpass'
        })
        end_time = time.time()
        
        response_time = (end_time - start_time) * 1000
        print(f"Время входа: {response_time:.2f} мс")
        
        assert response_time < 1000

if __name__ == "__main__":
    pytest.main([__file__, '-v'])