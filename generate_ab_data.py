"""
Генератор синтетических данных для A/B-тестирования в SQLite

Цель: 
    Создать реалистичную базу данных для анализа A/B-тестов, включая:
    - Пользователей и их характеристики
    - Эксперименты (название, период проведения)
    - Распределение пользователей по группам (control/treatment)
    - События пользователей (просмотры, клики, покупки)

Особенности:
    - Контролируемые вероятности конверсии для групп
    - Временные ограничения экспериментов
    - Связи между таблицами через FOREIGN KEY

Использование:
    Запуск скрипта создает файл ab_testing.db с готовыми данными.
"""

import sqlite3
import random
from datetime import datetime, timedelta
import os

# --- Конфигурация БД и данных ---
DB_NAME = 'ab_testing.db' # Имя файла вашей базы данных
NUM_USERS = 10000          # Количество пользователей, которые будут сгенерированы
NUM_EXPERIMENTS = 3        # Количество A/B экспериментов
NUM_DAYS_DATA = 90         # Генерируем данные о событиях за последние 90 дней

# Вероятности для генерации данных
DEVICE_TYPES = ['mobile', 'desktop', 'tablet'] # Типы устройств пользователей
REGIONS = ['North', 'South', 'East', 'West', 'Central'] # Географическое распределение
EVENT_TYPES = ['page_view', 'click', 'add_to_cart', 'purchase'] # Типы событий

# Вероятности конверсии для control и treatment (для демонстрации эффекта)
CONVERSION_RATES = {
    'control': {'add_to_cart': 0.15, 'purchase': 0.05}, # Базовые вероятности
    'treatment': {'add_to_cart': 0.18, 'purchase': 0.07} # Улучшенные вероятности
}

# --- Функции для генерации данных ---

def get_random_date(start_date, end_date):
    """Генерирует случайную дату и время между двумя заданными датами."""
    time_diff = end_date - start_date
    random_seconds = random.randint(0, int(time_diff.total_seconds()))
    return start_date + timedelta(seconds=random_seconds)

def create_connection(db_file):
    """Создает соединение с базой данных SQLite. Если файл не существует, он будет создан."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(f"Ошибка подключения к БД: {e}")
    return conn

def create_tables(conn):
    
    """    Создает структуру базы данных с 4 таблицами:
    1. users - информация о пользователях
    2. experiments - метаданные A/B-тестов
    3. experiment_assignments - распределение пользователей по группам
    4. user_events - действия пользователей

    Особенности:
    - Все таблицы связаны через FOREIGN KEY
    - experiment_assignments хранит variant (control/treatment)
    - user_events содержит разные типы данных в зависимости от event_type"""
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        created_at TEXT NOT NULL,
        region TEXT NOT NULL,
        device_type TEXT NOT NULL
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS experiments (
        experiment_id INTEGER PRIMARY KEY,
        experiment_name TEXT NOT NULL,
        description TEXT,
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        target_metric TEXT NOT NULL
    );
    ''')

    # ВАЖНОЕ ИСПРАВЛЕНИЕ: УДАЛЕНО AUTOINCREMENT.
    # В SQLite INTEGER PRIMARY KEY автоматически обеспечивает автоинкрементное поведение.
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS experiment_assignments (
        assignment_id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        experiment_id INTEGER NOT NULL,
        variant TEXT NOT NULL, -- 'control' or 'treatment'
        assigned_at TEXT NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users(user_id),
        FOREIGN KEY (experiment_id) REFERENCES experiments(experiment_id)
    );
    ''')

    # ВАЖНОЕ ИСПРАВЛЕНИЕ: УДАЛЕНО AUTOINCREMENT.
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_events (
        event_id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        event_timestamp TEXT NOT NULL,
        page_url TEXT,    -- Для событий 'page_view', 'click'
        item_id INTEGER,  -- Для событий 'add_to_cart', 'purchase'
        value REAL,       -- Для события 'purchase' (сумма покупки)
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    );
    ''')
    conn.commit()
    print("Таблицы успешно созданы или уже существуют.")

def generate_users(conn):
    """Генерирует и вставляет данные для таблицы users."""
    cursor = conn.cursor()
    users_data = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=NUM_DAYS_DATA + 30)

    for i in range(1, NUM_USERS + 1):
        created_at = get_random_date(start_date, end_date).isoformat()
        region = random.choice(REGIONS)
        device_type = random.choice(DEVICE_TYPES)
        users_data.append((i, created_at, region, device_type))

    cursor.executemany("INSERT INTO users VALUES (?, ?, ?, ?)", users_data)
    conn.commit()
    print(f"Сгенерировано {len(users_data)} пользователей.")

def generate_experiments(conn):
    """Генерирует и вставляет данные для таблицы experiments."""
    cursor = conn.cursor()
    experiments_data = []
    today = datetime.now()

    for i in range(1, NUM_EXPERIMENTS + 1):
        experiment_name = f"Experiment {i}: {'New Homepage Layout' if i == 1 else 'Button Color Test' if i == 2 else 'Checkout Flow Optimization'}"
        description = f"Testing {experiment_name} to improve {random.choice(['conversion rate', 'user engagement', 'retention'])}."
        start_date = today - timedelta(days=random.randint(5, NUM_DAYS_DATA))
        end_date = start_date + timedelta(days=random.randint(7, 30))
        target_metric = random.choice(['purchase_rate', 'click_through_rate', 'add_to_cart_rate', 'session_duration'])
        experiments_data.append((i, experiment_name, description, start_date.isoformat(), end_date.isoformat(), target_metric))

    cursor.executemany("INSERT INTO experiments VALUES (?, ?, ?, ?, ?, ?)", experiments_data)
    conn.commit()
    print(f"Сгенерировано {len(experiments_data)} экспериментов.")

def generate_experiment_assignments(conn):
    """    Распределяет пользователей по экспериментам и группам
    
    Логика:
    - Каждый пользователь может участвовать в 0-N экспериментах
    - В рамках одного эксперимента пользователь попадает 
      в control или treatment группу (50/50)
    - Назначение происходит в случайный момент времени 
      в рамках периода эксперимента"""
    cursor = conn.cursor()
    assignments_data = []

    cursor.execute("SELECT user_id FROM users")
    user_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT experiment_id, start_date, end_date FROM experiments")
    experiments = cursor.fetchall()

    for user_id in user_ids:
        num_assigned_experiments = random.randint(0, NUM_EXPERIMENTS)
        assigned_exp_ids = random.sample([exp[0] for exp in experiments], min(num_assigned_experiments, len(experiments)))

        for exp_id in assigned_exp_ids:
            exp_info = next((e for e in experiments if e[0] == exp_id), None)
            if not exp_info: continue

            exp_start_date = datetime.fromisoformat(exp_info[1])
            exp_end_date = datetime.fromisoformat(exp_info[2])

            assigned_at = get_random_date(exp_start_date, exp_end_date).isoformat()
            variant = random.choice(['control', 'treatment'])
            assignments_data.append((user_id, exp_id, variant, assigned_at))

    cursor.executemany("INSERT INTO experiment_assignments (user_id, experiment_id, variant, assigned_at) VALUES (?, ?, ?, ?)", assignments_data)
    conn.commit()
    print(f"Сгенерировано {len(assignments_data)} назначений экспериментов.")
    return assignments_data

def generate_user_events(conn, assignments_data):
    """Генерирует пользовательские события с учетом:
    - Принадлежности к группе (control/treatment)
    - Временных рамок экспериментов
    - Разных вероятностей конверсии для групп

    Особенности:
    - Для add_to_cart и purchase применяются вероятности из CONVERSION_RATES
    - page_view и click генерируются для всех пользователей
    - Покупки (purchase) имеют случайную сумму (value)"""
    cursor = conn.cursor()
    events_data = []
    
    today = datetime.now()
    data_start_date = today - timedelta(days=NUM_DAYS_DATA)

    user_variant_map = {}
    for assignment in assignments_data:
        user_id, experiment_id, variant, _ = assignment
        if user_id not in user_variant_map:
            user_variant_map[user_id] = {}
        user_variant_map[user_id][experiment_id] = variant

    for user_id in range(1, NUM_USERS + 1):
        num_events_per_user = random.randint(5, 50)
        
        current_variant = 'control'
        if user_id in user_variant_map:
            for exp_id, variant in user_variant_map[user_id].items():
                current_variant = variant
                break

        conversion_rates = CONVERSION_RATES[current_variant]

        for _ in range(num_events_per_user):
            event_timestamp = get_random_date(data_start_date, today).isoformat()
            event_type = random.choice(EVENT_TYPES)
            page_url = None
            item_id = None
            value = None

            if event_type == 'page_view':
                page_url = f"/page/{random.randint(1, 10)}"
            elif event_type == 'click':
                page_url = f"/button/{random.randint(1, 5)}"
            elif event_type == 'add_to_cart':
                if random.random() < conversion_rates['add_to_cart']:
                    item_id = random.randint(100, 999)
                else:
                    continue
            elif event_type == 'purchase':
                if random.random() < conversion_rates['purchase']:
                    item_id = random.randint(100, 999)
                    value = round(random.uniform(10.0, 500.0), 2)
                else:
                    continue

            events_data.append((user_id, event_type, event_timestamp, page_url, item_id, value))

    cursor.executemany("INSERT INTO user_events (user_id, event_type, event_timestamp, page_url, item_id, value) VALUES (?, ?, ?, ?, ?, ?)", events_data)
    conn.commit()
    print(f"Сгенерировано {len(events_data)} пользовательских событий.")

# --- Основной скрипт для запуска ---
if __name__ == "__main__":
    print(f"Текущая рабочая директория: {os.getcwd()}")

    if os.path.exists(DB_NAME):
        print(f"Файл '{DB_NAME}' уже существует. Удаляем его для перезаписи.")
        os.remove(DB_NAME)

    conn = create_connection(DB_NAME)
    if conn:
        create_tables(conn)
        generate_users(conn)
        generate_experiments(conn)

        assignments = generate_experiment_assignments(conn)
        generate_user_events(conn, assignments)

        conn.close()
        print(f"База данных '{DB_NAME}' успешно создана и заполнена данными.")
        print("Теперь вы можете подключиться к ней с помощью SQLite клиента или из Python.")