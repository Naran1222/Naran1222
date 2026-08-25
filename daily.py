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
    if not all([DB_HOST, DB_PORT, DB_USER, DB_PASS]):
        raise ValueError("Критична помилка: бракує змінних середовища для підключення до БД!")
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME)

def get_sheets_client():
    if not GOOGLE_CREDS_JSON:
        raise ValueError("Критична помилка: секрет ETL_SHEETS_BOT_CD не знайдено!")
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    return gspread.service_account_from_dict(creds_dict)

def get_last_8_days_data():
    """Виконує SQL-запит і повертає масив даних за останні 8 днів"""
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    WITH true_ftd AS (
        SELECT player_id, MIN(DATE(created_at)) AS ftd_date
        FROM invoices
        WHERE type = 1 AND status = 2
        GROUP BY player_id
    ),
    target_cohorts AS (
        SELECT player_id, ftd_date
        FROM true_ftd
        WHERE ftd_date >= CURRENT_DATE - INTERVAL '15 days'
    ),
    recent_deposits AS (
        SELECT d.player_id, DATE(d.created_at) AS deposit_date
        FROM invoices d
        JOIN target_cohorts c ON d.player_id = c.player_id
        WHERE d.type = 1 AND d.status = 2
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
    FROM target_cohorts f
    LEFT JOIN recent_deposits d ON f.player_id = d.player_id
    GROUP BY f.ftd_date
)
SELECT 
    day,
    cohort_size,
    d1, d2, d3, d4, d5, d6, d7,
    ROUND(SUM(d1) OVER w * 1.0 / NULLIF(SUM(cohort_size) OVER w, 0), 2) AS "d1%",
    ROUND(SUM(d2) OVER w * 1.0 / NULLIF(SUM(cohort_size) OVER w, 0), 2) AS "d2%",
    ROUND(SUM(d3) OVER w * 1.0 / NULLIF(SUM(cohort_size) OVER w, 0), 2) AS "d3%",
    ROUND(SUM(d4) OVER w * 1.0 / NULLIF(SUM(cohort_size) OVER w, 0), 2) AS "d4%",
    ROUND(SUM(d5) OVER w * 1.0 / NULLIF(SUM(cohort_size) OVER w, 0), 2) AS "d5%",
    ROUND(SUM(d6) OVER w * 1.0 / NULLIF(SUM(cohort_size) OVER w, 0), 2) AS "d6%",
    ROUND(SUM(d7) OVER w * 1.0 / NULLIF(SUM(cohort_size) OVER w, 0), 2) AS "d7%"

FROM cohort_activity
WINDOW w AS (ORDER BY day RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW)
ORDER BY day DESC;
    """
    
    cursor.execute(query)
    rows = cursor.fetchall() # Забираємо всі 8 рядків
    conn.close()
    
    if not rows:
        return None
        
    all_processed_rows = []
    for row in rows:
        processed_row = []
        for item in row:
            if item is None:
                processed_row.append(0)
            elif isinstance(item, Decimal):
                processed_row.append(float(item))
            elif isinstance(item, (datetime, date)):
                processed_row.append(item.strftime('%d.%m.%Y'))
            else:
                processed_row.append(item)
        all_processed_rows.append(processed_row)
            
    return all_processed_rows

def update_google_sheets(raw_data_rows):
    """Проходиться по масиву днів, оновлюючи або додаючи кожен рядок"""
    gc = get_sheets_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(WORKSHEET_NAME) 
    
    for raw_data_row in raw_data_rows:
        target_date = raw_data_row[0]
        # Витягуємо дати на кожній ітерації, щоб індекси не збилися при додаванні нових рядків
        dates_in_sheet = ws.col_values(2) 
        
        if target_date in dates_in_sheet:
            row_index = dates_in_sheet.index(target_date) + 1 
            existing_index = ws.cell(row_index, 1).value
            full_row = [existing_index] + raw_data_row
            cell_range = f"A{row_index}:Q{row_index}" 
            ws.update(cell_range, [full_row])
            print(f"[{datetime.now()}] Дані за {target_date} оновлено в рядку {row_index}.")
            
        else:
            all_col_a = ws.col_values(1)
            next_index = len(all_col_a) if len(all_col_a) > 0 else 1
            full_row = [next_index] + raw_data_row
            ws.append_row(full_row)
            print(f"[{datetime.now()}] Дані за {target_date} додано новим рядком (індекс {next_index}).")

if __name__ == "__main__":
    print("Запуск розрахунку когорт...")
    data_rows = get_last_8_days_data()
    
    if data_rows:
        update_google_sheets(data_rows)
    else:
        print("Немає даних для оновлення.")
