# excel_to_json_remote.py
import pandas as pd
import json
import os
import paramiko
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
def get_excel_data_from_server(server_ip, username, password):
    """
    Простая функция для получения двух JSON строк с данными
    """
    try:
        # Подключение к серверу
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server_ip, username=username, password=password)
        sftp = ssh.open_sftp()
        
        results = {}
        
        # Обработка employees.xlsx
        print("📖 Чтение employees.xlsx...")
        employees_file = sftp.file('/root/employees.xlsx', 'rb')
        employees_df = pd.read_excel(employees_file, engine='openpyxl')  # Добавлен engine
        employees_df = employees_df.where(pd.notnull(employees_df), None)
        employees_json = json.dumps(employees_df.to_dict('records'), ensure_ascii=False, default=str)
        results['employees'] = employees_json
        employees_file.close()
        print(f"✅ Employees: {len(employees_df)} записей")
        
        # Обработка tasks.xlsx
        print("📖 Чтение tasks.xlsx...")
        tasks_file = sftp.file('/root/tasks.xlsx', 'rb')
        tasks_df = pd.read_excel(tasks_file, engine='openpyxl')  # Добавлен engine
        tasks_df = tasks_df.where(pd.notnull(tasks_df), None)
        tasks_json = json.dumps(tasks_df.to_dict('records'), ensure_ascii=False, default=str)
        results['tasks'] = tasks_json
        tasks_file.close()
        print(f"✅ Tasks: {len(tasks_df)} записей")
        
        sftp.close()
        ssh.close()
        
        return results
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return {'employees': '[]', 'tasks': '[]'}

def data():
    """
    Функция для получения данных из Excel файлов на сервере
    """
    SERVER_IP = os.getenv('SERVER_IP')
    USERNAME = os.getenv('SERVER_USERNAME')
    PASSWORD = os.getenv('SERVER_PASSWORD')
    
    
    print("🚀 Получение данных с сервера...")
    return get_excel_data_from_server(SERVER_IP, USERNAME, PASSWORD)

if __name__ == "__main__":
    data_dict = data()
    employees_json = data_dict['employees']
    tasks_json = data_dict['tasks']
    
    print(f"📊 Employees JSON: {len(employees_json)} chars")
    print(f"📊 Tasks JSON: {len(tasks_json)} chars")