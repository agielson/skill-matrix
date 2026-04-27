import time
import concurrent.futures
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

class TestStressTesting:
    """Стресс-тестирование"""
    
    @patch('app.insert_user_data')
    @patch('app.get_user_data')
    def test_concurrent_registrations(self, mock_get_user, mock_insert_user, client):
        """Проверка работы при одновременных регистрациях"""
        mock_get_user.return_value = None
        mock_insert_user.return_value = (True, None)
        
        def make_registration(user_id):
            # Создаем новый клиент для каждого потока
            from app import app
            with app.test_client() as test_client:
                with app.app_context():
                    return test_client.post('/register', data={
                        'username': f'user_{user_id}',
                        'employee_id': f'EMP{user_id:04d}',
                        'password': 'SecurePass123'
                    })
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(make_registration, i) for i in range(10)]
            responses = []
            for f in futures:
                try:
                    responses.append(f.result(timeout=5))
                except Exception as e:
                    print(f"Error: {e}")
                    continue
        
        success_count = sum(1 for r in responses if r and r.status_code == 302)
        print(f"Успешных регистраций: {success_count} из 10")
        assert success_count >= 5
    
    @patch('app.get_user_data')
    def test_peak_login_load(self, mock_get_user, client):
        """Проверка работы при пиковой нагрузке на вход"""
        password_hash = bcrypt.hashpw(b'testpass', bcrypt.gensalt()).decode('utf-8')
        
        mock_get_user.return_value = {
            'username': 'test_user',
            'employee_id': 'EMP001',
            'password_hash': password_hash
        }
        
        def make_login():
            # Создаем новый клиент для каждого потока
            from app import app
            with app.test_client() as test_client:
                with app.app_context():
                    start_time = time.time()
                    response = test_client.post('/', data={
                        'username': 'test_user',
                        'password': 'testpass'
                    })
                    end_time = time.time()
                    return response, end_time - start_time
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(make_login) for _ in range(10)]
            results = []
            for f in futures:
                try:
                    results.append(f.result(timeout=5))
                except Exception as e:
                    print(f"Error: {e}")
                    continue
        
        response_times = [t for _, t in results if t]
        if response_times:
            avg_time = sum(response_times) / len(response_times)
            print(f"Среднее время входа: {avg_time:.3f} сек")
            assert avg_time < 3.0

if __name__ == "__main__":
    pytest.main([__file__, '-v'])