from flask import Flask, request, redirect, render_template_string
import jwt
import requests
import psycopg2
from datetime import datetime, timedelta, timezone
import bcrypt

app = Flask(__name__)

# app.py

# --- КОНФИГУРАЦИЯ ---
SUPERSET_SECRET_KEY = "TEST_NON_DEV_SECRET" 
SUPERSET_HOST = "http://127.0.0.1:8088"
DASHBOARD_ID = "1" # ID дашборда для просмотра
POSTGRES_URI = "dbname=db user=db_user password=db_password host=127.0.0.1 port=5432"

# Специальные учетные данные
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "admin"
REGULAR_USERNAME = "Regular"
REGULAR_PASSWORD = "user"
# --- КОНФИГУРАЦИЯ ---

def get_user_data(username):
    try:
        conn = psycopg2.connect(POSTGRES_URI)
        cur = conn.cursor()
        # Ищем пользователя по user_name в схеме dev
        cur.execute("SELECT password_hash, email, user_name FROM dev.flask_users WHERE user_name = %s", (username,))
        result = cur.fetchone()
        cur.close()
        conn.close()
        # Возвращаем словарь с данными
        return {"password_hash": result[0], "email": result[1], "username": result[2]} if result else None
    except Exception as e:
        print(f"Ошибка БД: {e}")
        return None

def create_guest_token(username, role, email=None):
    """Генерирует JWT для гостевого доступа к Superset."""
    
    # Все, кроме ADMIN, получают права "guest" (Viewer)
    scopes = ["guest"] 
    email = email if email else f"{username}@{role.lower()}.com"

    payload = {
        "user": {
            "username": username,
            "first_name": username,
            "last_name": role,
            "email": email,
        },
        "resource": [
            {"type": "dashboard", "id": DASHBOARD_ID},
        ],
        "scopes": scopes,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(minutes=5),
        "jti": f"{username}-{datetime.now().timestamp()}"
    }

    encoded_jwt = jwt.encode(payload, SUPERSET_SECRET_KEY, algorithm="HS256")
    return encoded_jwt

@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        # --- 1. ЛОГИКА АДМИНИСТРАТОРА (admin/admin) ---
        if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
            return redirect(f"{SUPERSET_HOST}") 
        
        # --- 2. ЛОГИКА СПЕЦИАЛЬНОГО VIEWWER (Regular/user) ---
        if username == REGULAR_USERNAME and password == REGULAR_PASSWORD:
            guest_token = create_guest_token("Regular Viewer", "Viewer", "regular@user.com")
            
            # Регистрируем токен и редиректим
            try:
                requests.post(
                    f"{SUPERSET_HOST}/api/v1/security/guest_token/",
                    json={"token": guest_token},
                    headers={"Content-Type": "application/json"}
                )
                return redirect(f"{SUPERSET_HOST}/embedded/dashboard/{DASHBOARD_ID}?guest=true&jwt={guest_token}")
            except requests.exceptions.ConnectionError:
                return "Ошибка подключения к Superset или Superset не принял токен.", 500

        
        user_data = get_user_data(username)
        
        if user_data:
            # Проверяем пароль с помощью bcrypt
            hashed_password = user_data["password_hash"].encode('utf-8')
            
            if bcrypt.checkpw(password.encode('utf-8'), hashed_password):
                guest_token = create_guest_token(user_data["username"], "Viewer", user_data["email"])
                
                # Регистрируем токен и редиректим
                try:
                    requests.post(
                        f"{SUPERSET_HOST}/api/v1/security/guest_token/",
                        json={"token": guest_token},
                        headers={"Content-Type": "application/json"}
                    )
                    return redirect(f"{SUPERSET_HOST}/embedded/dashboard/{DASHBOARD_ID}?guest=true&jwt={guest_token}")
                        
                except requests.exceptions.ConnectionError:
                    return "Ошибка подключения к Superset или Superset не принял токен.", 500
        
        # Если ни один из трех способов не сработал
        return "Неправильный логин или пароль", 401

       

    # Простая HTML-форма для логина
    return render_template_string("""
        <form method="post">
            <label for="username">Username:</label>
            <input type="text" id="username" name="username"><br><br>
            <label for="password">Password:</label>
            <input type="password" id="password" name="password"><br><br>
            <input type="submit" value="Login">
        </form>
        <p>Для редактирования: <b>admin/admin</b></p>
        <p>Для просмотра (Regular): <b>Regular/user</b></p>
        <p>Для просмотра (Сотрудники): <b>ivanov_ii/test</b> или <b>petrov_aa/test</b></p>
    """)

if __name__ == "__main__":
    app.run(debug=True, port=5000)