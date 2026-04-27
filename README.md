# Матрица компетенций

Система управления задачами и компетенциями сотрудников:
- `FastAPI` бэкенд (`fastapi_app`)
- мобильный клиент `Expo` (`mobile-app`)
- конвейер данных `Airflow -> parser -> PostgreSQL dev.*`

## Быстрый запуск

1. Поднимите инфраструктуру:

```bash
docker compose up -d db af fastapi
```

2. Проверьте API:

```bash
curl http://localhost:8001/health
```

3. Запустите мобильный клиент:

```bash
cd mobile-app
npm install
npm run start:lan
```

## Профиль production (только PostgreSQL)

Чтобы бэкенд работал только с PostgreSQL без перехода на SQLite:

1. Скопируйте шаблон env и заполните секреты:

```bash
cp .env.example .env
```

2. Убедитесь, что в `.env` установлено:
- `ALLOW_SQLITE_FALLBACK=false`
- `DATABASE_URL=postgresql+asyncpg://...`

3. Запустите сервисы:

```bash
docker compose up -d db af fastapi
```

4. Проверка:
- `GET /health` возвращает `ok`
- если PostgreSQL недоступен, FastAPI завершается с ошибкой (и не уходит в SQLite)

## Данные и парсинг

- Airflow DAG `skills_matrix` запускает загрузку данных из `parser/insert_records.py`.
- Парсер обновляет:
  - `dev.employees`
  - `dev.tasks`
  - `dev.task_status` (дефолтные статусы для всех задач)
  - `dev.employee_competency_level` (уровни компетенций по данным сотрудника)

## Основные возможности

- Роли `manager` / `employee`
- Создание задач менеджером
- Рекомендации задач для сотрудника (`ready_now` / `stretch_plus_one`)
- Предложение сотрудника на задачу менеджера (approve/reject)
- Уведомления с отметкой `прочитано` и `прочитать все`
- Фильтры задач по статусу и приоритету

## Демо-учетки

- `manager1 / 123456`
- `employee1 / 123456`

Если пароль не совпадает, запустите:

```bash
python fastapi_app/scripts/set_demo_credentials.py
```
