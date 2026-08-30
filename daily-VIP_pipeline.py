import os
import json
import pandas as pd
import psycopg2
import gspread
from gspread.utils import rowcol_to_a1
from datetime import datetime
import warnings

warnings.filterwarnings('ignore', category=UserWarning)

# --- ЗЧИТУВАННЯ КОНФІГУРАЦІЇ ---
# База 1
DB1_HOST = os.environ.get('SERVER_NAME')
DB1_PORT = os.environ.get('PORT')
DB1_USER = os.environ.get('USER_NAME')
DB1_PASS = os.environ.get('PASSWORD')
DB1_NAME = "infinity_wallet_service"

# База 2
DB2_HOST = os.environ.get('SERVER_NAME')
DB2_PORT = os.environ.get('PORT')
DB2_USER = os.environ.get('USER_NAME')
DB2_PASS = os.environ.get('PASSWORD')
DB2_NAME = "infinity_game_service"

# Google Sheets
GOOGLE_CREDS_JSON = os.environ.get('ETL_SHEETS_BOT_CD')
SPREADSHEET_ID = "1qODTcrrOY_ih7uQwgDrEyTYr1KaLhu4rMy-u_h9Rx4I"
JOIN_KEY = "player_id"

SQL_QUERY_1 = """
WITH 
vip_segment AS (
    SELECT UNNEST(ARRAY[
        '1923bd7f-b03c-447c-8824-8bcc794fe437', '55101c4e-3d2e-498a-b6a0-07db69de88ac', 
        'bb77cf7b-bdb7-4322-b155-1280c87d0059', '9a1628f5-ea05-4d5a-b96d-9be5d6998d1c', 
        'f6d66acc-4ea8-4f7e-9e88-a0aaf1413d4f', '77694558-39de-448f-8ff9-b2a72dbd832d', 
        'd9340c4a-60a5-4ad4-8535-103dcdfb4b53', '3c6cec32-3412-4956-8407-4e95259bd1b7', 
        '0f65ef4e-67c5-4ec0-bd92-ba0f2d6adc14'
    ]::uuid[]) AS player_id
),

-- (Блоки base_tx, deposits, wd_stats, gameplay_stats залишаються ТАКИМИ Ж, як були)
base_tx AS (
    SELECT 
        a.player_id,
        t.reference_type,
        t.type,
        t.created_at,
        (t.base_currency_real_amount + COALESCE(t.base_currency_bonus_amount, 0) + COALESCE(t.base_currency_lock_amount, 0)) / 100.0 AS amount_total
    FROM public.transactions t
    JOIN public.accounts a ON t.account_id = a.id
    JOIN vip_segment v ON a.player_id = v.player_id
    WHERE t.created_at >= current_date - interval '1 day' AND t.created_at < current_date
),
deposits AS (
    SELECT 
        player_id,
        created_at, 
        amount_total,
        ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY created_at ASC) as rn
    FROM base_tx
    WHERE reference_type = 'payment.deposit' AND type = 1
),
dep_stats AS (
    SELECT 
        player_id,
        COUNT(*) AS deps_count,
        SUM(amount_total) AS deps_sum,
        AVG(amount_total) AS deps_avg,
        SUM(CASE WHEN rn = 1 THEN amount_total ELSE 0 END) AS first_dep_amount,
        MIN(created_at) AS first_dep_time,
        CASE WHEN COUNT(*) > 1 
             THEN (SUM(amount_total) - SUM(CASE WHEN rn = 1 THEN amount_total ELSE 0 END)) / (COUNT(*) - 1)
             ELSE 0 
        END AS avg_repeat_dep
    FROM deposits
    GROUP BY player_id
),
wd_stats AS (
    SELECT player_id, COUNT(*) AS wd_count, SUM(amount_total) AS wd_sum
    FROM base_tx
    WHERE reference_type = 'payment.withdrawal' AND type = 1
    GROUP BY player_id
),
gameplay_stats AS (
    SELECT 
        player_id,
        SUM(CASE WHEN reference_type = 'bet.placebet' THEN 1 ELSE 0 END) - SUM(CASE WHEN reference_type = 'bet.rollback' THEN 1 ELSE 0 END) AS total_spins,
        SUM(CASE WHEN reference_type = 'bet.placebet' THEN amount_total ELSE 0 END) - SUM(CASE WHEN reference_type = 'bet.rollback' THEN amount_total ELSE 0 END) AS total_bets,
        SUM(CASE WHEN reference_type = 'bet.settle' AND type = 1 THEN amount_total ELSE 0 END) AS total_wins,
        MAX(CASE WHEN reference_type = 'bet.placebet' THEN amount_total ELSE 0 END) AS max_bet,
        SUM(CASE WHEN reference_type = 'bonus.finish' THEN amount_total ELSE 0 END) AS bonus_payout
    FROM base_tx
    GROUP BY player_id
)

-- ==========================================
-- ФІНАЛЬНИЙ SELECT
-- ==========================================
SELECT 
    v.player_id,
    
    COALESCE(ds.deps_count, 0) AS deps_count,
    COALESCE(ds.deps_sum, 0) AS deps_sum,
    COALESCE(ds.deps_avg, 0) AS deps_avg,
    
    -- НОВИЙ БЛОК: Склеюємо суму і час
    CASE 
        WHEN ds.first_dep_time IS NOT NULL 
        THEN ROUND(ds.first_dep_amount, 2)::text || ' at ' || TO_CHAR(ds.first_dep_time, 'YYYY-MM-DD HH24:MI:SS')
        ELSE '0' 
    END AS first_dep,
    
    COALESCE(ds.avg_repeat_dep, 0) AS avg_repeat_dep,
    
    COALESCE(ws.wd_count, 0) AS wd_count,
    COALESCE(ws.wd_sum, 0) AS wd_sum,
    
    COALESCE(gs.total_spins, 0) AS total_spins,
    COALESCE(gs.total_bets, 0) AS total_bets,
    COALESCE(gs.total_wins, 0) AS total_wins,
    COALESCE(gs.total_bets, 0) - COALESCE(gs.total_wins, 0) AS ggr,
    (COALESCE(gs.total_bets, 0) - COALESCE(gs.total_wins, 0)) - COALESCE(gs.bonus_payout, 0) AS ngr,
    CASE WHEN COALESCE(gs.total_bets, 0) <= 0 THEN 0 ELSE (COALESCE(gs.total_wins, 0) / gs.total_bets) * 100 END AS rtp,
    COALESCE(gs.max_bet, 0) AS max_bet,
    COALESCE(gs.bonus_payout, 0) AS bonus_payout

FROM 
    vip_segment v
LEFT JOIN 
    dep_stats ds ON v.player_id = ds.player_id
LEFT JOIN 
    wd_stats ws ON v.player_id = ws.player_id
LEFT JOIN 
    gameplay_stats gs ON v.player_id = gs.player_id;
"""

SQL_QUERY_2 = """
WITH 
vip_segment AS (
    SELECT UNNEST(ARRAY[
        '1923bd7f-b03c-447c-8824-8bcc794fe437', '55101c4e-3d2e-498a-b6a0-07db69de88ac', 
        'bb77cf7b-bdb7-4322-b155-1280c87d0059', '9a1628f5-ea05-4d5a-b96d-9be5d6998d1c', 
        'f6d66acc-4ea8-4f7e-9e88-a0aaf1413d4f', '77694558-39de-448f-8ff9-b2a72dbd832d', 
        'd9340c4a-60a5-4ad4-8535-103dcdfb4b53', '3c6cec32-3412-4956-8407-4e95259bd1b7', 
        '0f65ef4e-67c5-4ec0-bd92-ba0f2d6adc14'
    ]::uuid[]) AS player_id
),
sessions_data AS (
    SELECT 
        player_id,
        MIN(created_at) AS first_session_start,
        MAX(COALESCE(expired_at, created_at)) AS last_session_end 
    FROM 
        public.game_sessions
    WHERE 
        created_at >= current_date - interval '1 day'
        AND created_at < current_date
        AND is_demo_mode = false
    GROUP BY 
        player_id
),
yesterday_bets AS (
    SELECT 
        player_id,
        game_id,
        spin_base_currency_amount / 100.0 AS bet_amount 
    FROM 
        public.spins
    WHERE 
        created_at >= current_date - interval '1 day'
        AND created_at < current_date
),
-- Групуємо ставки по кожній грі для гравця
game_totals AS (
    SELECT 
        yb.player_id,
        g.name AS game_name,
        SUM(yb.bet_amount) AS total_bet_on_game
    FROM yesterday_bets yb
    LEFT JOIN public.games g ON yb.game_id = g.id
    GROUP BY yb.player_id, g.name
),
-- Ранжуємо ігри, щоб знайти Топ-1, та формуємо загальний рядок
games_aggregated AS (
    SELECT 
        player_id,
        -- Витягуємо назву та суму гри з найбільшою ставкою (rn = 1)
        MAX(CASE WHEN rn = 1 THEN game_name ELSE NULL END) AS top_game_name,
        MAX(CASE WHEN rn = 1 THEN total_bet_on_game ELSE 0 END) AS top_game_bet,
        -- Збираємо всі ігри у форматі "Назва: 100 | Назва2: 50"
        STRING_AGG(game_name || ': ' || ROUND(total_bet_on_game, 2)::text, ' | ') AS all_games_list
    FROM (
        SELECT 
            player_id,
            game_name,
            total_bet_on_game,
            ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY total_bet_on_game DESC) as rn
        FROM game_totals
    ) ranked
    GROUP BY player_id
)

SELECT 
    v.player_id,
    ga.top_game_name,
    ga.top_game_bet,
    ga.all_games_list,
    sd.first_session_start,
    sd.last_session_end
FROM 
    vip_segment v
LEFT JOIN 
    games_aggregated ga ON v.player_id = ga.player_id
LEFT JOIN 
    sessions_data sd ON v.player_id = sd.player_id;
"""
PLAYER_NAMES = {
    "1923bd7f-b03c-447c-8824-8bcc794fe437": "Ian Raduly-turnbull",
    "55101c4e-3d2e-498a-b6a0-07db69de88ac": "julie collins",
    "bb77cf7b-bdb7-4322-b155-1280c87d0059": "Greg Paulsen",
    "9a1628f5-ea05-4d5a-b96d-9be5d6998d1c": "Danielle Aucoin",
    "f6d66acc-4ea8-4f7e-9e88-a0aaf1413d4f": "Trevor Shand",
    "77694558-39de-448f-8ff9-b2a72dbd832d": "Hazel Greene",
    "d9340c4a-60a5-4ad4-8535-103dcdfb4b53": "Chyrel Lestage",
    "3c6cec32-3412-4956-8407-4e95259bd1b7": "garret johnson",
    "0f65ef4e-67c5-4ec0-bd92-ba0f2d6adc14": "Mathieu Livingstone ВИП"
}


def get_db_connection(host, port, user, password, dbname):
    if not all([host, port, user, password, dbname]):
        raise ValueError(f"Критична помилка: бракує змінних середовища для підключення до {dbname}!")
    return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)

def fetch_data(query, host, port, user, password, dbname):
    """Підключення до бази, виконання запиту і повернення DataFrame."""
    conn = get_db_connection(host, port, user, password, dbname)
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_sheets_client():
    if not GOOGLE_CREDS_JSON:
        raise ValueError("Критична помилка: секрет ETL_SHEETS_BOT_CD не знайдено!")
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    return gspread.service_account_from_dict(creds_dict)

def main():
    print(f"[{datetime.now()}] Витягуємо дані з баз...")
    df1 = fetch_data(SQL_QUERY_1, DB1_HOST, DB1_PORT, DB1_USER, DB1_PASS, DB1_NAME)
    df2 = fetch_data(SQL_QUERY_2, DB2_HOST, DB2_PORT, DB2_USER, DB2_PASS, DB2_NAME)

    print(f"[{datetime.now()}] Робимо JOIN...")
    df_joined = pd.merge(df1, df2, on=JOIN_KEY, how="outer")
    # Видаляємо стару лінію з astype(str).replace(...)
    df_joined.set_index(JOIN_KEY, inplace=True)
    
    print(f"[{datetime.now()}] Підключаємось до Google Sheets...")
    gc = get_sheets_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    run_date = datetime.now().strftime("%d.%m.%Y")

    # Допоміжна функція: примусово робить рядок і вичищає всі види NaN/Inf
    def get_safe_str(val):
        if pd.isna(val):
            return ""
        v = str(val).strip()
        if v.lower() in ("nan", "nat", "none", "<na>", "inf", "-inf"):
            return ""
        return v

    for player_id in df_joined.index:
        clean_player_id = str(player_id) 
        sheet_name = PLAYER_NAMES.get(clean_player_id, clean_player_id)
        
        row_data = df_joined.loc[player_id].to_dict()
        
        try:
            worksheet = sh.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=sheet_name, rows="100", cols="50")
            print(f"[{datetime.now()}] Створено новий аркуш для гравця {sheet_name}")

        first_row = worksheet.row_values(1)
        next_col_idx = len(first_row) + 1

        if next_col_idx == 1:
            metrics_col = [
                ["Дата"], ["Email"], ["Последний вход в систему"], ["Последняя активность на сайте"],
                ["Что сделал"], ["Player ID"], ["Dep Count"], ["Total Dep"], ["Avg Dep"],
                ["First Dep"], ["Avg Redep"], ["Withdrawals Count"], ["Total Withdraw"],
                ["Total Bets"], ["Total Wins"], ["GGR"], ["NGR"], ["Player RTP"],
                ["Max Bet"], ["Top Game (Provider)"]
            ]
            worksheet.update(values=metrics_col, range_name=rowcol_to_a1(1, 1))
            next_col_idx = 2

        # Застосовуємо get_safe_str() до кожної метрики, що гарантує валідний JSON
        player_col_formatted = [
            [run_date],                                    
            [""],                                          
            [get_safe_str(row_data.get("first_session_start"))],     
            [get_safe_str(row_data.get("last_session_end"))],        
            [get_safe_str(row_data.get("all_games_list"))],          
            [clean_player_id],                             
            [get_safe_str(row_data.get("deps_count"))],              
            [get_safe_str(row_data.get("deps_sum"))],                
            [get_safe_str(row_data.get("deps_avg"))],                
            [get_safe_str(row_data.get("first_dep"))],               
            [get_safe_str(row_data.get("avg_repeat_dep"))],          
            [get_safe_str(row_data.get("wd_count"))],                
            [get_safe_str(row_data.get("wd_sum"))],                  
            [get_safe_str(row_data.get("total_bets"))],              
            [get_safe_str(row_data.get("total_wins"))],              
            [get_safe_str(row_data.get("ggr"))],                     
            [get_safe_str(row_data.get("ngr"))],                     
            [get_safe_str(row_data.get("rtp"))],                     
            [get_safe_str(row_data.get("max_bet"))],                 
            [get_safe_str(row_data.get("top_game_name"))]            
        ]

        start_cell = rowcol_to_a1(1, next_col_idx)
        worksheet.update(values=player_col_formatted, range_name=start_cell)
        print(f"[{datetime.now()}] Гравець {sheet_name}: дані додано у стовпець {start_cell}")

    print(f"[{datetime.now()}] Пайплайн успішно завершено!")

if __name__ == "__main__":
    main()
