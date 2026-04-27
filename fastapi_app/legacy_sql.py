"""Helpers for legacy dev.* tables: PostgreSQL uses schema-qualified names; SQLite needs quoted identifiers."""


def legacy_table_names(dialect_name: str) -> dict[str, str]:
    if dialect_name == "sqlite":
        return {
            "flask_users": '"dev.flask_users"',
            "employees": '"dev.employees"',
            "tasks": '"dev.tasks"',
            "task_status": '"dev.task_status"',
        }
    return {
        "flask_users": "dev.flask_users",
        "employees": "dev.employees",
        "tasks": "dev.tasks",
        "task_status": "dev.task_status",
    }


def sql_timestamp_default() -> str:
    """Works on PostgreSQL and SQLite."""
    return "CURRENT_TIMESTAMP"
