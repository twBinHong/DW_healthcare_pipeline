from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Variable, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.notifications.smtp import send_smtp_notification


from datetime import datetime, timedelta
import pandas as pd 


@task(
    retries=1,                              # 失敗時最多自動重試 1 次
    retry_delay=timedelta(minutes=2),          # 每次重試間隔 2 分鐘)
)
def ingest_file():
    file_path = Variable.get("path_healthcare")
        
    try:
        # 3. 使用 pandas 讀取檔案並取得總行數
        df = pd.read_csv(file_path)
        total_rows = len(df)
    except Exception as e:
        total_rows = 0
        print(f"Warning: Could not read {file_path}. Error: {e}")
    
    return file_path

@task(
    retries=1,                              # 失敗時最多自動重試 1 次
    retry_delay=timedelta(minutes=2),          # 每次重試間隔 2 分鐘)
)
def validate_data(file_path: str):
    print(f"[Validate Data] 開始讀取並檢查 CSV 檔案路徑: {file_path}")
    
    try:
        # 指定預期的欄位 Header 確保讀取結構正確
        expected_headers = [
            "patient_id", "age", "gender", "symptoms", 
            "illness_name", "medicines", "nearest_hospital", "pharmacy"
        ]
        df = pd.read_csv(file_path)
    except Exception as e:
        raise AirflowException(f"無法讀取 CSV 檔案 ({file_path})。錯誤原因: {e}")
        
    if df.empty:
        print("[Validate Data] 警告：CSV 檔案內容為空，無資料可驗證。")
        return []

    total_rows = len(df)
    invalid_row_indices = set()
    
    # -------------------------------------------------------------------------
    # 1. 檢查 patient_id 是否有重複（若重複，將該組所有重複列視為異常）
    # -------------------------------------------------------------------------
    duplicate_mask = df.duplicated(subset=['patient_id'], keep=False)
    duplicate_indices = df[duplicate_mask].index.tolist()
    for idx in duplicate_indices:
        invalid_row_indices.add(idx)
        
    # -------------------------------------------------------------------------
    # 2. 檢查 age 是否有負值
    # -------------------------------------------------------------------------
    negative_age_mask = df['age'] < 0
    negative_age_indices = df[negative_age_mask].index.tolist()
    for idx in negative_age_indices:
        invalid_row_indices.add(idx)
        
    # -------------------------------------------------------------------------
    # 3. 檢查各欄位是否有空值 (NaN / Null)
    # -------------------------------------------------------------------------
    # 檢查上述所有預期欄位中是否存在空值
    existing_cols = [col for col in expected_headers if col in df.columns]
    null_mask = df[existing_cols].isnull().any(axis=1)
    null_indices = df[null_mask].index.tolist()
    for idx in null_indices:
        invalid_row_indices.add(idx)
        
    # -------------------------------------------------------------------------
    # 4. 記錄有問題的列數（轉換為 1-based 的 CSV 行號，包含 Header 為第 1 行）
    # -------------------------------------------------------------------------
    # DataFrame 的 index 0 對應 CSV 檔案的第 2 行
    invalid_csv_row_numbers = [idx + 2 for idx in sorted(list(invalid_row_indices))]
    
    if invalid_csv_row_numbers:
        print(f"[Validate Data] 偵測到異常資料的 CSV 行號（含 Header）: {invalid_csv_row_numbers}")
    else:
        print("[Validate Data] 檢查完畢，未發現任何異常資料！")

    # -------------------------------------------------------------------------
    # 5. 刪除有問題的列，並重新重設 Index
    # -------------------------------------------------------------------------
    clean_df = df.drop(index=list(invalid_row_indices)).reset_index(drop=True)
    
    valid_rows_count = len(clean_df)
    removed_rows_count = len(invalid_row_indices)
    
    # Print 資料清理的摘要
    print("\n" + "="*45)
    print("             資料驗證與清理摘要報告")
    print("="*45)
    print(f" • 原始總筆數       : {total_rows} 筆")
    print(f" • 異常被刪除筆數   : {removed_rows_count} 筆")
    print(f" • 有效保留筆數     : {valid_rows_count} 筆")
    print(f" • 異常列號清單     : {invalid_csv_row_numbers if invalid_csv_row_numbers else '無'}")
    print("="*45 + "\n")
    
    # -------------------------------------------------------------------------
    # 6. 將整理完的資料轉換為 dict (orient="records") 以 Task API 方式回傳
    # -------------------------------------------------------------------------
    return clean_df.to_dict(orient="records")

@task(
    retries=2,                              # 失敗時最多自動重試 2 次
    retry_delay=timedelta(minutes=2),          # 每次重試間隔 2 分鐘)
)
def upload_staging_area(valid_records: list[dict]):
    print(f"[Upload Staging] 準備將清洗後的資料上傳至 PostgreSQL (toll.staging_area)，總筆數: {len(valid_records)}")
    
    if not valid_records:
        print("[Upload Staging] 警告：無有效資料可上傳。")
        return 0

    # 1. 初始化 PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    
    try:
        # 2. 檢查 toll schema 是否存在
        check_schema_sql = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name = 'toll';
        """
        schema_df = postgres_hook.get_pandas_df(check_schema_sql)
        
        if schema_df.empty:
            raise AirflowException("Postgres 錯誤：schema 'toll' 不存在，請先由管理員建立或授權。")
        
        print("[Upload Staging] 確認 schema 'toll' 存在。")

        
        # 設定 patient_id 為 PRIMARY KEY
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS toll.staging_area (
            patient_id VARCHAR(50) PRIMARY KEY,
            age INT,
            gender VARCHAR(20),
            symptoms VARCHAR(200),
            illness_name VARCHAR(200),
            medicines VARCHAR(200),
            nearest_hospital VARCHAR(200),
            pharmacy VARCHAR(100),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        postgres_hook.run(create_table_sql)
        print("[Upload Staging] 成功檢查/建立表格 'toll.staging_area'。")

        # 4. 準備批次上傳
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        # 將傳入的 dict 轉換為 tuple 清單
        rows = [
            (
                str(row.get("patient_id")),
                row.get("age"),
                row.get("gender"),
                row.get("symptoms"),
                row.get("illness_name"),
                row.get("medicines"),
                row.get("nearest_hospital"),
                row.get("pharmacy")
            )
            for row in valid_records
        ]

        
        # 撰寫支援 ON CONFLICT (Upsert) 的標準 SQL 語句
        insert_query = """
        INSERT INTO toll.staging_area (
            patient_id, age, gender, symptoms, illness_name, medicines, nearest_hospital, pharmacy
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (patient_id) 
        DO UPDATE SET 
            age = EXCLUDED.age,
            gender = EXCLUDED.gender,
            symptoms = EXCLUDED.symptoms,
            illness_name = EXCLUDED.illness_name,
            medicines = EXCLUDED.medicines,
            nearest_hospital = EXCLUDED.nearest_hospital,
            pharmacy = EXCLUDED.pharmacy,
            updated_at = CURRENT_TIMESTAMP;
        """

        
        cursor.executemany(insert_query, rows)
        conn.commit()
        cursor.close()
        conn.close()

        # 5. 總結上傳資料
        uploaded_count = len(rows)
        print("\n" + "="*45)
        print("         PostgreSQL Staging 區上傳摘要報告")
        print("="*45)
        print(f" • 目標 Schema & Table : toll.staging_area")
        print(f" • 處理/同步總筆數     : {uploaded_count} 筆")
        print("="*45 + "\n")

        return uploaded_count

    except Exception as e:
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        raise AirflowException(f"上傳至 PostgreSQL Staging 失敗: {e}")

def modeling_dim():
    print("[Data Modeling] 開始執行維度資料建模（Dimension Modeling）...")
    
    # 1. 初始化 PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    
    try:
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        # 2. 建立五張維度表格（設定各自主鍵與 UNIQUE 約束）
        create_dims_sql = """
        --病人維度表
        CREATE TABLE IF NOT EXISTS toll.dimpatient (
            patient_id VARCHAR(20) PRIMARY KEY,
            age INT,
            gender VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 症狀維度表
        CREATE TABLE IF NOT EXISTS toll.dimsymptom (
            symptom_id SERIAL PRIMARY KEY,
            symptom_name VARCHAR(200) UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 疾病維度表
        CREATE TABLE IF NOT EXISTS toll.dimillness (
            illness_id SERIAL PRIMARY KEY,
            illness_name VARCHAR(200) UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 藥品維度表（包含單價欄位）
        CREATE TABLE IF NOT EXISTS toll.dimmedicine (
            medicine_id SERIAL PRIMARY KEY,
            medicine_name VARCHAR(200) UNIQUE,
            unitprice NUMERIC(10, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 醫院維度表
        CREATE TABLE IF NOT EXISTS toll.dimhospital (
            hospital_id SERIAL PRIMARY KEY,
            hospital_name VARCHAR(200) UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 藥局維度表
        CREATE TABLE IF NOT EXISTS toll.dimpharmacy (
            pharmacy_id SERIAL PRIMARY KEY,
            pharmacy_name VARCHAR(200) UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_dims_sql)
        conn.commit()
        print("[Data Modeling] 成功檢查/建立所有維度表格（dimpatient, dimsymptom, dimillness, dimmedicine, dimhospital, dimpharmacy）。")

        # 3. 從 toll.staging_area 萃取寫入維度表 (ON CONFLICT DO NOTHING)
        
        # (1) 症狀維度
        cursor.execute("""
            INSERT INTO toll.dimsymptom (symptom_name)
            SELECT DISTINCT symptoms FROM toll.staging_area 
            WHERE symptoms IS NOT NULL AND symptoms != ''
            ON CONFLICT (symptom_name) DO NOTHING;
        """)

        # (2) 疾病維度
        cursor.execute("""
            INSERT INTO toll.dimillness (illness_name)
            SELECT DISTINCT illness_name FROM toll.staging_area 
            WHERE illness_name IS NOT NULL AND illness_name != ''
            ON CONFLICT (illness_name) DO NOTHING;
        """)

        # (3) 藥品維度
        cursor.execute("""
            INSERT INTO toll.dimmedicine (medicine_name)
            SELECT DISTINCT medicines FROM toll.staging_area 
            WHERE medicines IS NOT NULL AND medicines != ''
            ON CONFLICT (medicine_name) DO NOTHING;
        """)

        # 4. 針對 toll.dimmedicine 計算 unitprice (id * 10)
        # 僅針對尚未賦值（unitprice IS NULL）的新增藥品進行計算，確保歷史資料不被覆蓋
        cursor.execute("""
            UPDATE toll.dimmedicine 
            SET unitprice = medicine_id * 10 
            WHERE unitprice IS NULL;
        """)

        # (5) 醫院維度
        cursor.execute("""
            INSERT INTO toll.dimhospital (hospital_name)
            SELECT DISTINCT nearest_hospital FROM toll.staging_area 
            WHERE nearest_hospital IS NOT NULL AND nearest_hospital != ''
            ON CONFLICT (hospital_name) DO NOTHING;
        """)

        # (6) 藥局維度
        cursor.execute("""
            INSERT INTO toll.dimpharmacy (pharmacy_name)
            SELECT DISTINCT pharmacy FROM toll.staging_area 
            WHERE pharmacy IS NOT NULL AND pharmacy != ''
            ON CONFLICT (pharmacy_name) DO NOTHING;
        """)

        # (7) 病人維度
        cursor.execute("""
            INSERT INTO toll.dimpatient (patient_id, age, gender)
            SELECT patient_id, age, gender FROM toll.staging_area 
            WHERE patient_id IS NOT NULL AND patient_id != ''
            ON CONFLICT (patient_id) DO NOTHING;
        """)

        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "="*45)
        print("          資料倉儲維度建模摘要報告")
        print("="*45)
        print(" • 已完成維度表同步：dimsymptom, dimillness, dimmedicine, dimhospital, dimpharmacy, dimpatient")
        print("="*45 + "\n")

        return "Dimension modeling completed successfully."

    except Exception as e:
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        raise AirflowException(f"維度資料建模失敗: {e}")

def modeling_fact():
    print("[Fact Modeling] 開始執行事實表建模（Fact Table Modeling: toll.factvisit）...")
    
    # 1. 初始化 PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    
    try:
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        # 2. 建立 toll.factvisit 事實表格
        # 包含業務邏輯所需的各維度 ID、計算欄位，並設定複合唯一鍵
        create_fact_sql = """
        CREATE TABLE IF NOT EXISTS toll.factvisit (
            id SERIAL PRIMARY KEY,
            patient_id VARCHAR(50),
            symptom_id INT,
            illness_id INT,
            medicine_id INT,
            unit INT,
            amount NUMERIC(10, 2),
            hospital_id INT,
            pharmacy_id INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            -- 建立複合唯一約束，防止重複匯入相同病患的相同診療紀錄
            CONSTRAINT unique_patient_visit_key UNIQUE (patient_id, symptom_id, illness_id, medicine_id)
        );
        """
        cursor.execute(create_fact_sql)
        conn.commit()
        print("[Fact Modeling] 成功檢查/建立事實表格 'toll.factvisit'。")

        # 3. 從 staging_area 與各大維度表進行關聯查詢，計算邏輯並寫入事實表
        # - unit: 依據 age / 100 無條件進位 (CEIL)
        # - amount: unit * unitprice (取自 toll.dimmedicines)
        insert_fact_sql = """
        INSERT INTO toll.factvisit (
            patient_id, 
            symptom_id, 
            illness_id, 
            medicine_id, 
            unit, 
            amount, 
            hospital_id, 
            pharmacy_id
        )
        SELECT 
            s.patient_id,
            ds.symptom_id,
            di.illness_id,
            dm.medicine_id,
            --  藥品數量 unit = age / 100 無條件進位
            CEIL(s.age::NUMERIC / 100.0)::INT AS unit,
            --  amount = unit * unitprice
            (CEIL(s.age::NUMERIC / 100.0) * dm.unitprice) AS amount,
            dh.hospital_id,
            dp.pharmacy_id
        FROM toll.staging_area s
        LEFT JOIN toll.dimsymptom ds ON s.symptoms = ds.symptom_name
        LEFT JOIN toll.dimillness di ON s.illness_name = di.illness_name
        LEFT JOIN toll.dimmedicine dm ON s.medicines = dm.medicine_name
        LEFT JOIN toll.dimhospital dh ON s.nearest_hospital = dh.hospital_name
        LEFT JOIN toll.dimpharmacy dp ON s.pharmacy = dp.pharmacy_name
        ORDER BY s.patient_id ASC
        ON CONFLICT (patient_id, symptom_id, illness_id, medicine_id) 
        DO UPDATE SET 
            unit = EXCLUDED.unit,
            amount = EXCLUDED.amount,
            hospital_id = EXCLUDED.hospital_id,
            pharmacy_id = EXCLUDED.pharmacy_id,
            created_at = CURRENT_TIMESTAMP;
        """
        
        cursor.execute(insert_fact_sql)
        conn.commit()
        
        # 取得影響筆數
        row_count = cursor.rowcount
        
        cursor.close()
        conn.close()

        print("\n" + "="*45)
        print("          事實表建模摘要報告")
        print("="*45)
        print(f" • 目標表格           : toll.factvisit")
        print(f" • 處理/同步紀錄筆數   : {row_count} 筆")
        print("="*45 + "\n")

        return f"Fact modeling completed successfully, processed {row_count} rows."

    except Exception as e:
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        raise AirflowException(f"事實表建模失敗: {e}")

def validate_data_quality():
    print("[Data Quality] 開始驗證 toll.factvisit 資料品質...")

    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    issues = []

    try:
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # 0. 確認事實表是否有資料
        cursor.execute("SELECT COUNT(*) FROM toll.factvisit;")
        total_fact_rows = cursor.fetchone()[0]
        print(f"[Data Quality] toll.factvisit 目前總筆數: {total_fact_rows}")

        # 1. 檢查維度關聯完整性（確保外鍵 ID 皆非 NULL 且存在於對應維度表）
        orphan_checks_sql = """
        SELECT
            COUNT(*) FILTER (WHERE f.symptom_id IS NULL OR ds.symptom_id IS NULL) AS missing_symptom,
            COUNT(*) FILTER (WHERE f.illness_id IS NULL OR di.illness_id IS NULL) AS missing_illness,
            COUNT(*) FILTER (WHERE f.medicine_id IS NULL OR dm.medicine_id IS NULL) AS missing_medicine,
            COUNT(*) FILTER (WHERE f.hospital_id IS NULL OR dh.hospital_id IS NULL) AS missing_hospital,
            COUNT(*) FILTER (WHERE f.pharmacy_id IS NULL OR dp.pharmacy_id IS NULL) AS missing_pharmacy,
            COUNT(*) FILTER (WHERE f.patient_id IS NULL OR dpa.patient_id IS NULL) AS missing_patient
        FROM toll.factvisit f
        LEFT JOIN toll.dimsymptom ds ON f.symptom_id = ds.symptom_id
        LEFT JOIN toll.dimillness di ON f.illness_id = di.illness_id
        LEFT JOIN toll.dimmedicine dm ON f.medicine_id = dm.medicine_id
        LEFT JOIN toll.dimhospital dh ON f.hospital_id = dh.hospital_id
        LEFT JOIN toll.dimpharmacy dp ON f.pharmacy_id = dp.pharmacy_id
        LEFT JOIN toll.dimpatient dpa ON f.patient_id = dpa.patient_id;
        """
        cursor.execute(orphan_checks_sql)
        (
            missing_symptom,
            missing_illness,
            missing_medicine,
            missing_hospital,
            missing_pharmacy,
            missing_patient,
        ) = cursor.fetchone()

        orphan_summary = {
            "symptom_id": missing_symptom,
            "illness_id": missing_illness,
            "medicine_id": missing_medicine,
            "hospital_id": missing_hospital,
            "pharmacy_id": missing_pharmacy,
            "patient_id":missing_patient,
        }
        total_orphan_rows = sum(orphan_summary.values())

        if total_orphan_rows > 0:
            issues.append(f"維度關聯異常（缺漏/孤兒鍵筆數）: {orphan_summary}")

        # 2. 檢查金額計算邏輯（amount 應等於 unit * dm.unitprice，容許極小浮點誤差）
        amount_mismatch_sql = """
        SELECT COUNT(*)
        FROM toll.factvisit f
        JOIN toll.dimmedicine dm ON f.medicine_id = dm.medicine_id
        WHERE f.unit IS NULL
           OR f.amount IS NULL
           OR dm.unitprice IS NULL
           OR ABS(f.amount - (f.unit * dm.unitprice)) > 0.01;
        """
        cursor.execute(amount_mismatch_sql)
        amount_mismatch_count = cursor.fetchone()[0]

        if amount_mismatch_count > 0:
            issues.append(f"金額計算不一致筆數（amount != unit * unitprice 或為 NULL）: {amount_mismatch_count}")

        # 3. 檢查複合業務鍵是否有重複紀錄（patient_id, symptom_id, illness_id, medicine_id）
        duplicate_fact_sql = """
        SELECT COUNT(*)
        FROM (
            SELECT patient_id, symptom_id, illness_id, medicine_id
            FROM toll.factvisit
            GROUP BY patient_id, symptom_id, illness_id, medicine_id
            HAVING COUNT(*) > 1
        ) AS dup_groups;
        """
        cursor.execute(duplicate_fact_sql)
        duplicate_group_count = cursor.fetchone()[0]

        if duplicate_group_count > 0:
            issues.append(f"事實表重複組合組數（patient_id, symptom_id, illness_id, medicine_id）: {duplicate_group_count}")

        cursor.close()
        conn.close()

        # 4. 印出驗證摘要報告
        print("\n" + "=" * 50)
        print("          toll.factvisit 資料品質驗證摘要")
        print("=" * 50)
        print(f" • 事實表總筆數           : {total_fact_rows}")
        print(f" • 維度關聯異常筆數合計   : {total_orphan_rows} -> {orphan_summary}")
        print(f" • 金額計算異常筆數       : {amount_mismatch_count}")
        print(f" • 重複業務鍵組數         : {duplicate_group_count}")
        print(f" • 驗證結果               : {'通過 (PASSED)' if not issues else '失敗 (FAILED)'}")
        print("=" * 50 + "\n")

        if issues:
            raise AirflowException("資料品質驗證未通過:\n - " + "\n - ".join(issues))

        return {
            "status": "passed",
            "total_fact_rows": total_fact_rows,
            "amount_mismatch_count": amount_mismatch_count,
            "duplicate_group_count": duplicate_group_count,
        }

    except AirflowException:
        if "conn" in locals() and conn:
            conn.close()
        raise
    except Exception as e:
        if "conn" in locals() and conn:
            conn.rollback()
            conn.close()
        raise AirflowException(f"執行事實表資料品質驗證時發生錯誤: {e}")

with DAG(
    dag_id="DW_healthcare_pipeline",
    start_date=datetime(2026,11,1),
    schedule='@monthly',
    catchup=False,
    default_args={'owner': 'BinHong'},
    description="data pipeline to process ETL and data modeling",
    on_failure_callback=[
        send_smtp_notification(
            from_email="airflow@example.com",
            to="your_email@example.com",
            subject="Airflow Alert: Task Failed",
            html_content="The task {{ ti.task_id }} failed in DAG {{ dag.dag_id }}.",
        )
    ],
) as dag:

    start = EmptyOperator(task_id="start")
    
    file_path = ingest_file()
    clean_date = validate_data(file_path)
    upload_staging = upload_staging_area(clean_date)
    
    modeling_dim = PythonOperator(
        task_id="modeling_dimension",
        python_callable=modeling_dim,
        retries=2,
        retry_delay=timedelta(minutes=2)
    )

    modeling_fact =  PythonOperator(
        task_id="modeling_fact",
        python_callable=modeling_fact,
        retries=2,
        retry_delay=timedelta(minutes=2)
    )

    data_quality = PythonOperator(
        task_id="data_quality",
        python_callable=validate_data_quality
    )

    end = EmptyOperator(
        task_id="end",
        #trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start >> file_path >> clean_date >> upload_staging 
    upload_staging >> modeling_dim >> modeling_fact >> data_quality >> end