from flask import Flask, request, redirect, render_template_string, url_for, send_file
import psycopg2
import bcrypt
import os
from pathlib import Path

app = Flask(__name__)

# --- КОНФИГУРАЦИЯ ---
SUPERSET_HOST = "http://127.0.0.1:8088"
DASHBOARD_URL = "/superset/dashboard/p/LE01l49mbwM/" 
POSTGRES_URI = "dbname=db user=db_user password=db_password host=localhost port=5432" 
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "admin"
# Путь к фону логина: каталог приложения + images/, либо переменная окружения FLASK_AUTH_BG_IMAGE
_BASE_DIR = Path(__file__).resolve().parent
ABSOLUTE_IMAGE_PATH = os.environ.get(
    "FLASK_AUTH_BG_IMAGE",
    str(_BASE_DIR / "images" / "bussines.jpg"),
)
# --- КОНФИГУРАЦИЯ ---


# ✅ МАРШРУТ: Специально для обслуживания изображения по абсолютному пути
@app.route('/images/bussines.jpg')
def serve_bussines_image():
    """Обслуживает файл по абсолютному пути, указанному в ABSOLUTE_IMAGE_PATH."""
    try:
        # Используем send_file для отправки файла по абсолютному пути.
        return send_file(ABSOLUTE_IMAGE_PATH, mimetype='image/jpeg')
    except OSError:
        # Если файл не найден по абсолютному пути, возвращаем 404.
        print(f"File not found at: {ABSOLUTE_IMAGE_PATH}")
        return "Изображение не найдено", 404
    except Exception as e:
        print(f"Error serving image: {e}")
        return "Внутренняя ошибка сервера", 500


# --- ФУНКЦИИ БАЗЫ ДАННЫХ ---
def connect_db():
    return psycopg2.connect(POSTGRES_URI)

def get_user_data(username):
    try:
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("SELECT password_hash, user_name, employee_id FROM dev.flask_users WHERE user_name = %s", (username,))
        result = cur.fetchone()
        cur.close()
        conn.close()
        return {"password_hash": result[0], "username": result[1], "employee_id": result[2]} if result else None
    except Exception as e:
        print(f"Database error while fetching user: {e}")
        return None

# Исправленная версия insert_user_data - возвращает кортеж (success, error_message)
def insert_user_data(username, employee_id, password):
    try:
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        conn = connect_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO dev.flask_users (user_name, employee_id, password_hash) VALUES (%s, %s, %s)", 
            (username, employee_id, hashed_password)
        )
        conn.commit()
        cur.close()
        conn.close()
        return True, None  # Успех, сообщение об ошибке отсутствует
    except psycopg2.errors.UniqueViolation:
        return False, "Пользователь уже существует или идентификатор сотрудника уже зарегистрирован"
    except Exception as e:
        print(f"Database error while registering user: {e}")
        return False, str(e)  # Возвращаем ошибку


# --- МАРШРУТЫ ---
@app.route("/", methods=["GET", "POST"])
def login():
    message = ""
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        
        if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
            return redirect(f"{SUPERSET_HOST}") 
        
        user_data = get_user_data(username)
        
        if user_data:
            hashed_password = user_data["password_hash"].encode('utf-8')
            
            if bcrypt.checkpw(password.encode('utf-8'), hashed_password):
                return redirect(f"{SUPERSET_HOST}{DASHBOARD_URL}")
            else:
                message = "Неправильный пароль."
        else:
            message = "Пользователь не найден."

        if not message:
             message = "Неправильный логин или пароль."
    
    return render_template_string(LOGIN_TEMPLATE, message=message)


@app.route("/register", methods=["GET", "POST"])
def register():
    message = ""
    if request.method == "POST":
        username = request.form.get("username")
        employee_id = request.form.get("employee_id") 
        password = request.form.get("password")
        
        if not (username and employee_id and password):
            message = "Все поля должны быть заполнены."
        elif get_user_data(username):
            message = "Пользователь с таким именем уже существует."
        else:
            # ✅ Теперь result - это кортеж (success, error_msg)
            success, error_msg = insert_user_data(username, employee_id, password)
            
            if success:
                return redirect(url_for('login', registered=True))
            elif error_msg and "Пользователь уже существует" in error_msg:
                message = "Пользователь с таким именем или идентификатором уже существует."
            else:
                message = f"Ошибка при регистрации: {error_msg}" if error_msg else "Ошибка при регистрации. Проверьте подключение к БД."

    if request.args.get('registered'):
        message = "Регистрация прошла успешно! Теперь войдите в систему."

    return render_template_string(REGISTER_TEMPLATE, message=message)


# --- HTML-ШАБЛОНЫ ---
LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Вход в систему</title>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700;900&display=swap" rel="stylesheet">
    <style>
        /* Цвета */
        :root {
            --color-primary: #38761D;
            --color-secondary: #F4F0E0;
            --color-text: #1C2718;
            --color-error: #CC0000;
            --color-success: #10b981;
            --bg-image: url('/images/bussines.jpg'); 
        }

        body { 
            font-family: 'Montserrat', sans-serif; 
            background-color: #f4f7f9; 
            display: flex; 
            flex-direction: column; 
            justify-content: center; 
            align-items: center; 
            height: 100vh; 
            margin: 0; 
            background-image: var(--bg-image);
            background-size: cover;
            background-position: center;
        }
        
        .container { 
            background: rgba(255, 255, 255, 0.2); 
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px); 
            padding: 30px; 
            border-radius: 12px; 
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3); 
            width: 350px; 
            text-align: center; 
            border: 1px solid rgba(255, 255, 255, 0.3); 
        }
        
        h2 { 
            color: var(--color-primary); 
            font-size: 1.8em;
            margin-top: 0;
            margin-bottom: 10px; 
            font-weight: 900;
            text-align: center;
        }

        h3 { 
            color: var(--color-primary); 
            font-size: 1.2em;
            margin-top: 0; 
            margin-bottom: 25px; 
            font-weight: 700;
        }

        label { 
            display: block; 
            margin-bottom: 8px; 
            font-weight: 600; 
            text-align: left; 
            color: var(--color-text); 
        }

        input[type="text"], input[type="password"] { 
            width: 100%; 
            padding: 12px; 
            margin-bottom: 20px; 
            border: 1px solid #cbd5e1; 
            border-radius: 8px; 
            box-sizing: border-box; 
            transition: border-color 0.3s; 
            background: rgba(255, 255, 255, 0.8); 
        }

        input[type="text"]:focus, input[type="password"]:focus { 
            border-color: var(--color-primary); 
            outline: none; 
            box-shadow: 0 0 0 3px rgba(56, 118, 29, 0.3); 
        }

        .btn-primary { 
            background-color: var(--color-primary); 
            color: var(--color-secondary); 
            padding: 12px 15px; 
            border: none; 
            border-radius: 8px; 
            cursor: pointer; 
            font-size: 16px; 
            width: 100%; 
            font-weight: 700; 
            transition: background-color 0.3s; 
        }

        .btn-primary:hover { 
            background-color: #274E13; 
        }

        .message { 
            color: var(--color-error); 
            margin-bottom: 15px; 
            font-weight: 600; 
            background: rgba(255, 220, 220, 0.8);
            padding: 10px;
            border-radius: 5px;
        }

        .info { 
            color: var(--color-success); 
            margin-bottom: 15px; 
            font-weight: 600; 
            background: rgba(220, 255, 220, 0.8);
            padding: 10px;
            border-radius: 5px;
        }

        .small-text { 
            margin-top: 25px; 
            font-size: 0.9em; 
            color: var(--color-text); 
        }

        .small-text a { 
            color: var(--color-primary); 
            text-decoration: none; 
            font-weight: 700; 
        }

        .small-text a:hover { 
            text-decoration: underline; 
        }
    </style>
</head>
<body>
    <div class="container">
        <h2>Матрица компетенций</h2>
        <h3>Вход в систему</h3>
        
        {% if message %}
            <div class="{{ 'message' if 'Ошибка' in message or 'Неправильный' in message else 'info' }}">
                {{ message }}
            </div>
        {% endif %}
        
        <form method="post" action="/">
            <label for="username">Имя пользователя:</label>
            <input type="text" id="username" name="username" required><br>
            <label for="password">Пароль:</label>
            <input type="password" id="password" name="password" required><br>
            <button type="submit" class="btn-primary">Войти</button>
        </form>

        <div class="small-text">
            Нет аккаунта? <a href="{{ url_for('register') }}">Зарегистрироваться</a>
        </div>
    </div>
</body>
</html>
"""

REGISTER_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Регистрация</title>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700;900&display=swap" rel="stylesheet">
    <style>
        :root {
            --color-primary: #38761D;
            --color-secondary: #F4F0E0;
            --color-text: #1C2718;
            --color-error: #CC0000;
            --color-success: #10b981;
            --bg-image: url('/images/bussines.jpg'); 
        }

        body { 
            font-family: 'Montserrat', sans-serif; 
            background-color: #f4f7f9; 
            display: flex; 
            flex-direction: column; 
            justify-content: center; 
            align-items: center; 
            height: 100vh; 
            margin: 0; 
            background-image: var(--bg-image);
            background-size: cover;
            background-position: center;
        }
        
        .container { 
            background: rgba(255, 255, 255, 0.2); 
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px); 
            padding: 30px; 
            border-radius: 12px; 
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3); 
            width: 350px; 
            text-align: center; 
            border: 1px solid rgba(255, 255, 255, 0.3);
        }

        h3 { 
            color: var(--color-primary); 
            font-size: 1.2em;
            margin-top: 0; 
            margin-bottom: 25px; 
            font-weight: 700;
        }

        label { 
            display: block; 
            margin-bottom: 8px; 
            font-weight: 600; 
            text-align: left; 
            color: var(--color-text); 
        }

        input[type="text"], input[type="password"] { 
            width: 100%; 
            padding: 12px; 
            margin-bottom: 20px; 
            border: 1px solid #cbd5e1; 
            border-radius: 8px; 
            box-sizing: border-box; 
            transition: border-color 0.3s; 
            background: rgba(255, 255, 255, 0.8);
        }

        input[type="text"]:focus, input[type="password"]:focus { 
            border-color: var(--color-primary); 
            outline: none; 
            box-shadow: 0 0 0 3px rgba(56, 118, 29, 0.3); 
        }

        .btn-primary { 
            background-color: var(--color-primary); 
            color: var(--color-secondary); 
            padding: 12px 15px; 
            border: none; 
            border-radius: 8px; 
            cursor: pointer; 
            font-size: 16px; 
            width: 100%; 
            font-weight: 700; 
            transition: background-color 0.3s; 
        }

        .btn-primary:hover { 
            background-color: #274E13; 
        }

        .message { 
            color: var(--color-error); 
            margin-bottom: 15px; 
            font-weight: 600; 
            background: rgba(255, 220, 220, 0.8);
            padding: 10px;
            border-radius: 5px;
        }

        .info { 
            color: var(--color-success); 
            margin-bottom: 15px; 
            font-weight: 600; 
            background: rgba(220, 255, 220, 0.8);
            padding: 10px;
            border-radius: 5px;
        }

        .small-text { 
            margin-top: 25px; 
            font-size: 0.9em; 
            color: var(--color-text); 
        }

        .small-text a { 
            color: var(--color-primary); 
            text-decoration: none; 
            font-weight: 700; 
        }

        .small-text a:hover { 
            text-decoration: underline; 
        }
    </style>
</head>
<body>
    <div class="container">
        <h3>Регистрация пользователя</h3>

        {% if message %}
            <div class="{{ 'message' if 'Ошибка' in message or 'существует' in message else 'info' }}">
                {{ message }}
            </div>
        {% endif %}

        <form method="post" action="/register">
            <label for="username">Имя пользователя:</label>
            <input type="text" id="username" name="username" required><br>
            <label for="employee_id">Идентификатор сотрудника:</label>
            <input type="text" id="employee_id" name="employee_id" required><br> 
            <label for="password">Пароль:</label>
            <input type="password" id="password" name="password" required><br>
            <button type="submit" class="btn-primary">Зарегистрироваться</button>
        </form>

        <div class="small-text">
            Уже есть аккаунт? <a href="{{ url_for('login') }}">Войти</a>
        </div>
    </div>
</body>
</html>
"""

if __name__ == "__main__":
    app.run(debug=True, port=5000)