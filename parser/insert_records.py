# insert_records.py
from generate import data
import psycopg2
import json
import os
from dotenv import load_dotenv

load_dotenv()

def connect_to_db():
    print("🔌 Connecting to the Postgres database...")
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        print("✅ Database connected successfully")
        return conn
    except psycopg2.Error as e:
        print(f"❌ Database connection failed: {e}")
        raise

def create_tables(conn):
    print("🗄️ Creating tables if not exist...")
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            
            CREATE TABLE IF NOT EXISTS dev.employees (
                id SERIAL PRIMARY KEY,
                country VARCHAR(100) NOT NULL,
                factory VARCHAR(100) NOT NULL,
                department VARCHAR(100) NOT NULL,
                team VARCHAR(100) NOT NULL,
                employee_full_name VARCHAR(200) NOT NULL,
                employee_id VARCHAR(50) UNIQUE NOT NULL,
                position VARCHAR(100) NOT NULL,
                age INTEGER NOT NULL CHECK (age >= 18 AND age <= 70),
                gender VARCHAR(20) NOT NULL,
                competencies TEXT,
                planned_vacation_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS dev.tasks (
                id SERIAL PRIMARY KEY,
                task_id VARCHAR(50) UNIQUE NOT NULL,
                task_name VARCHAR(200) NOT NULL,
                department VARCHAR(100) NOT NULL,
                team VARCHAR(100) NOT NULL,
                factory VARCHAR(100) NOT NULL,
                country VARCHAR(100) NOT NULL,
                required_competency VARCHAR(100) NOT NULL,
                required_position VARCHAR(100) NOT NULL,
                deadline DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        print("✅ Tables were created successfully")
    except psycopg2.Error as e:
        print(f"❌ Failed to create tables: {e}")
        raise

def insert_employees(conn, employees_json):
    print("👥 Inserting employees data...")
    try:
        employees_data = json.loads(employees_json)
        cursor = conn.cursor()
        
        inserted_count = 0
        for employee in employees_data:
            cursor.execute("""
                INSERT INTO dev.employees (
                    country, factory, department, team, employee_full_name,
                    employee_id, position, age, gender, competencies, planned_vacation_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (employee_id) DO UPDATE SET
                    country = EXCLUDED.country,
                    factory = EXCLUDED.factory,
                    department = EXCLUDED.department,
                    team = EXCLUDED.team,
                    employee_full_name = EXCLUDED.employee_full_name,
                    position = EXCLUDED.position,
                    age = EXCLUDED.age,
                    gender = EXCLUDED.gender,
                    competencies = EXCLUDED.competencies,
                    planned_vacation_date = EXCLUDED.planned_vacation_date,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                employee.get('Country'),
                employee.get('Factory'), 
                employee.get('Department'),
                employee.get('Team'),
                employee.get('Employee_Full_Name'),
                employee.get('Employee_ID'),
                employee.get('Position'),
                employee.get('Age'),
                employee.get('Gender'),
                employee.get('Competencies'),
                employee.get('Vacation_Date')
            ))
            inserted_count += 1
        
        conn.commit()
        print(f"✅ Inserted/updated {inserted_count} employees")
        return inserted_count
    except psycopg2.Error as e:
        print(f"❌ Error inserting employees: {e}")
        conn.rollback()
        return 0

def insert_tasks(conn, tasks_json):
    print("📝 Inserting tasks data...")
    try:
        tasks_data = json.loads(tasks_json)
        cursor = conn.cursor()
        
        inserted_count = 0
        for task in tasks_data:
            cursor.execute("""
                INSERT INTO dev.tasks (
                    task_id, task_name, department, team, factory,
                    country, required_competency, required_position, deadline
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (task_id) DO UPDATE SET
                    task_name = EXCLUDED.task_name,
                    department = EXCLUDED.department,
                    team = EXCLUDED.team,
                    factory = EXCLUDED.factory,
                    country = EXCLUDED.country,
                    required_competency = EXCLUDED.required_competency,
                    required_position = EXCLUDED.required_position,
                    deadline = EXCLUDED.deadline
            """, (
                task.get('Task_ID'),
                task.get('Task_Name'),
                task.get('Department'),
                task.get('Team'),
                task.get('Factory'),
                task.get('Country'),
                task.get('Required_Competency'),
                task.get('Required_Position'),
                task.get('Deadline')
            ))
            inserted_count += 1
        
        conn.commit()
        print(f"✅ Inserted/updated {inserted_count} tasks")
        return inserted_count
    except psycopg2.Error as e:
        print(f"❌ Error inserting tasks: {e}")
        conn.rollback()
        return 0

def get_stats(conn):
    """Получение статистики по данным"""
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM dev.employees")
        employees_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dev.tasks") 
        tasks_count = cursor.fetchone()[0]
        
        print(f"\n📊 Database Statistics:")
        print(f"   Employees: {employees_count}")
        print(f"   Tasks: {tasks_count}")
        
    except psycopg2.Error as e:
        print(f"❌ Error getting stats: {e}")

def main():
    conn = None
    try:
        
        # Получаем данные из excel_to_json_remote
        data_dict = data()
        employees_json = data_dict['employees']
        tasks_json = data_dict['tasks']
        
        # Проверяем что данные не пустые
        employees_data = json.loads(employees_json)
        tasks_data = json.loads(tasks_json)
        
        print(f"📥 Получено {len(employees_data)} employees и {len(tasks_data)} tasks")
        
        if len(employees_data) == 0 or len(tasks_data) == 0:
            print("❌ Нет данных для вставки")
            return "NO_DATA"  # Добавьте возвращаемое значение
        
        # Подключаемся и работаем с БД
        conn = connect_to_db()
        create_tables(conn)
        
        # Вставляем данные
        employees_count = insert_employees(conn, employees_json)
        tasks_count = insert_tasks(conn, tasks_json)
        
        # Показываем статистику
        get_stats(conn)
        
        print(f"\n🎉 All operations completed successfully!")
        print(f"   Employees processed: {employees_count}")
        print(f"   Tasks processed: {tasks_count}")
        
        return f"SUCCESS: {employees_count} employees, {tasks_count} tasks"
        
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return f"ERROR: {str(e)}"
    finally:
        if conn:
            conn.close()
            print('🔌 Database connection closed.')

if __name__ == "__main__":
    main()