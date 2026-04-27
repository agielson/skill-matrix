import pytest
from unittest.mock import patch, MagicMock

@pytest.fixture(autouse=True)
def mock_db_connections():
    """Мок для всех подключений к БД по умолчанию"""
    with patch('app.connect_db') as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        yield mock_connect