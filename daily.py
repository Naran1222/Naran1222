import os
import json
import psycopg2
import gspread
from datetime import datetime, date
from decimal import Decimal

# --- ЗЧИТУВАННЯ КОНФІГУРАЦІЇ ---
DB_HOST = os.environ.get('SERVER_NAME')
DB_PORT = os.environ.get('PORT')
DB_USER = os.environ.get('USER_NAME')
DB_PASS = os.environ.get('PASSWORD')
GOOGLE_CREDS_JSON = os.environ.get('ETL_SHEETS_BOT_CD')

DB_NAME = "infinity_payment_service" 
SPREADSHEET_NAME = "Daily Eval"
WORKSHEET_NAME = "Raw"

def get_db_connection():
    """Ініціалізація з'єднання з PostgreSQL"""
    if not all([DB_HOST, DB_PORT, DB_USER, DB_PASS]):
        raise ValueError("Критична помилка: бракує змінних середовища для підключення до БД!")
        
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME
    )

def get_sheets_client():
    """Ініціалізація клієнта Google Sheets через JSON-секрет"""
    if not GOOGLE_CREDS_JSON:
        raise ValueError("Критична помилка: секрет ETL_SHEETS_BOT_CD не знайдено!")
        
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    return gspread.service_account_from_dict(creds_dict)

def get_yesterday_data():
    """Виконує SQL-запит і повертає 1 рядок даних за вчора"""
    # ВИКОРИСТОВУЄМО НАШУ ФУНКЦІЮ
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    WITH recent_deposits AS (
        SELECT player_id, DATE(created_at) AS deposit_date
        FROM invoices
        WHERE type = 'deposit' 
          AND status = 'success'
          AND created_at >= CURRENT_DATE - INTERVAL '15 days'
    ),
    ftd_dates AS (
        SELECT player_id, MIN(deposit_date) AS ftd_date
        FROM recent_deposits
        GROUP BY player_id
    ),
    cohort_activity AS (
        SELECT 
            f.ftd_date AS day,
            COUNT(DISTINCT f.player_id) AS cohort_size,
            COUNT(DISTINCT CASE WHEN d.deposit_date - f.ftd_date = 1 THEN d.player_id END) AS d1,
            COUNT(DISTINCT CASE WHEN d.deposit_date - f.ftd_date = 2 THEN d.player_id END) AS d2,
            COUNT(DISTINCT CASE WHEN d.deposit_date - f.ftd_date = 3 THEN d.player_id END) AS d3,
            COUNT(DISTINCT CASE WHEN d.deposit_date - f.ftd_date = 4 THEN d.player_id END) AS d4,
            COUNT(DISTINCT CASE WHEN d.deposit_date - f.ftd_date = 5 THEN d.player_id END) AS d5,
            COUNT(DISTINCT CASE WHEN d.deposit_date - f.ftd_date = 6 THEN d.player_id END) AS d6,
            COUNT(DISTINCT CASE WHEN d.deposit_date - f.ftd_date = 7 THEN d.player_id END) AS d7
        FROM ftd_dates f
        LEFT JOIN recent_deposits d ON f.player_id = d.player_id
        GROUP BY f.ftd_date
    ),
    rolling_calc AS (
        SELECT 
            day, cohort_size, d1, d2, d3, d4, d5, d6, d7,
            ROUND(SUM(d1) OVER w * 1.0 / NULLIF(SUM(cohort_size) OVER w, 0), 4) AS "d1%",
            ROUND(SUM(d2) OVER w * 1.0 / NULLIF(SUM(cohort_size) OVER w, 0), 4) AS "d2%",
            ROUND(SUM(d3) OVER w * 1.0 / NULLIF(SUM(cohort_size) OVER w, 0), 4) AS "d3%",
            ROUND(SUM(d4) OVER w * 1.0 / NULLIF(SUM(cohort_size) OVER w, 0), 4) AS "d4%",
            ROUND(SUM(d5) OVER w * 1.0 / NULLIF(SUM(cohort_size) OVER w, 0), 4) AS "d5%",
            ROUND(SUM(d6) OVER w * 1.0 / NULLIF(SUM(cohort_size) OVER w, 0), 4) AS "d6%",
            ROUND(SUM(d7) OVER w * 1.0 / NULLIF(SUM(cohort_size) OVER w, 0), 4) AS "d7%"
        FROM cohort_activity
        WINDOW w AS (ORDER BY day RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW)
    )
    SELECT * FROM rolling_calc 
    WHERE day = CURRENT_DATE - INTERVAL '1 day';
    """
    
    cursor.execute(query)
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return None
        
    # Правильне форматування типів даних для Google Sheets
    processed_row = []
    for item in row:
        if item is None:
            processed_row.append(0)
        elif isinstance(item, Decimal):
            processed_row.append(float(item)) # Відсотки стануть float
        elif isinstance(item, (datetime, date)):
            processed_row.append(str(item))   # Дати стануть текстом
        else:
            processed_row.append(item)        # Цілі числа (cohort_size, d1-d7) залишаться цілими
            
    return processed_row

def update_google_sheets(new_row):
    """Апендить або перезаписує дані в Google Sheets"""
    # ВИКОРИСТОВУЄМО НАШУ ФУНКЦІЮ
    gc = get_sheets_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(WORKSHEET_NAME)
    
    target_date = new_row[0] 
    
    dates_in_sheet = ws.col_values(1)
    
    if target_date in dates_in_sheet:
        row_index = dates_in_sheet.index(target_date) + 1 
        cell_range = f"A{row_index}:P{row_index}" 
        ws.update(cell_range, [new_row])
        print(f"[{datetime.now()}] Дані за {target_date} оновлено в рядку {row_index}.")
    else:
        ws.append_row(new_row)
        print(f"[{datetime.now()}] Дані за {target_date} додано новим рядком.")

if __name__ == "__main__":
    print("Запуск розрахунку когорт...")
    data = get_yesterday_data()
    
    if data:
        update_google_sheets(data)
    else:
        print("Немає даних за вчорашній день.")