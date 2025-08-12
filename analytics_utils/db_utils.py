# db_utils.py
import re
from datetime import datetime

import psycopg
import pymysql

import pandas as pd
from ipynb.fs.full.config import POSTGRES_CREDS, MYSQL_CREDS

def test_postgres_connection(creds=POSTGRES_CREDS) -> bool:
    try:
        conn = psycopg.connect(**creds)
        conn.close()
        print("✅ PostgreSQL connection successful.")
        return True
    except Exception as e:
        print("❌ PostgreSQL connection failed:", e)
        return False

def test_mysql_connection(creds=MYSQL_CREDS) -> bool:
    try:
        conn = pymysql.connect(
            host=creds["host"],
            user=creds["user"],
            password=creds["password"],
            port=creds["port"]
        )
        conn.close()
        print("✅ MySQL connection successful.")
        return True
    except Exception as e:
        print("❌ MySQL connection failed:", e)
        return False
    
# --- Data Queries ---
def run_email_engagement_query(program_ids, creds=POSTGRES_CREDS):
    if isinstance(program_ids, int):
        program_ids = [program_ids]

    placeholders = ','.join(['%s'] * len(program_ids))
    query = f"""
        SELECT *, 'email_engagement' as source
        FROM dbt.email_engagements ee
        WHERE ee.program_identifier IN ({placeholders})
    """

    try:
        with psycopg.connect(**creds) as conn:
            with conn.cursor() as cur:
                cur.execute(query, program_ids)
                rows = cur.fetchall()
                colnames = [desc.name for desc in cur.description]
                df = pd.DataFrame(rows, columns=colnames)
                print(f"✅ Pulled {len(df)} email engagements for {program_ids}")
                return df
    except Exception as e:
        print("❌ Email engagement query failed:", e)
        return pd.DataFrame()

def run_asset_view_query(program_ids, creds=POSTGRES_CREDS):
    if isinstance(program_ids, int):
        program_ids = [program_ids]

    placeholders = ','.join(['%s'] * len(program_ids))
    query = f"""
        SELECT *, 'asset_view' as source
        FROM dbt.asset_views av
        WHERE av.program_identifier IN ({placeholders})
    """

    try:
        with psycopg.connect(**creds) as conn:
            with conn.cursor() as cur:
                cur.execute(query, program_ids)
                rows = cur.fetchall()
                colnames = [desc.name for desc in cur.description]
                df = pd.DataFrame(rows, columns=colnames)
                print(f"✅ Pulled {len(df)} asset views for {program_ids}")
                return df
    except Exception as e:
        print("❌ Asset view query failed:", e)
        return pd.DataFrame()

def run_survey_response_query(program_ids, creds=POSTGRES_CREDS):
    """
    Fetch survey responses for one or more program_identifiers.
    """
    if isinstance(program_ids, int):
        program_ids = [program_ids]

    placeholders = ','.join(['%s'] * len(program_ids))
    query = f"""
        SELECT *, 'survey_response' as source
        FROM dbt.survey_responses sr
        WHERE sr.program_identifier IN ({placeholders})
    """

    try:
        with psycopg.connect(**creds) as conn:
            with conn.cursor() as cur:
                cur.execute(query, program_ids)
                rows = cur.fetchall()
                colnames = [desc.name for desc in cur.description]
                df = pd.DataFrame(rows, columns=colnames)
                print(f"✅ Pulled {len(df)} survey responses for {program_ids}")
                return df
    except Exception as e:
        print("❌ Survey response query failed:", e)
        return pd.DataFrame()
    
def run_adbutler_banner_impression_query(program_ids, creds=POSTGRES_CREDS):
    """
    Fetch AdButler banner ad impressions from dbt.banner_ad_impressions.
    """
    if isinstance(program_ids, int):
        program_ids = [program_ids]

    placeholders = ','.join(['%s'] * len(program_ids))
    query = f"""
        SELECT *, 'adbutler_banner_ad' as source
        FROM dbt.banner_ad_impressions bai
        WHERE bai.program_identifier IN ({placeholders})
    """

    try:
        with psycopg.connect(**creds) as conn:
            with conn.cursor() as cur:
                cur.execute(query, program_ids)
                rows = cur.fetchall()
                colnames = [desc.name for desc in cur.description]
                df = pd.DataFrame(rows, columns=colnames)
                print(f"✅ Pulled {len(df)} AdButler banner impressions for {program_ids}")
                return df
    except Exception as e:
        print("❌ AdButler banner impression query failed:", e)
        return pd.DataFrame()
    
def run_choozle_banner_engagement_query(program_ids, creds=MYSQL_CREDS):
    """
    Fetch Choozle banner ad impressions and clicks for one or more program IDs.
    Filters to users in BTL concentrate. Returns a pandas DataFrame.
    """
    if isinstance(program_ids, int):
        program_ids = [program_ids]

    placeholders = ','.join(['%s'] * len(program_ids))

    query = f"""
    SELECT
        PRG_ID AS program_identifier,
        npiNumber AS provider_npi,
        MAX(date) AS engaged_on,
        ad_name AS choozle_banner_ad_name,
        CASE
            WHEN activity = 'Ad View' THEN 'choozle_banner_impression'
            ELSE 'choozle_banner_click'
        END AS engagement_type,
        destination_url AS choozle_banner_link_url,
        'choozle_banner_ad' AS source
    FROM healthst_media.tbl_banner_data
    WHERE PRG_ID IN ({placeholders})
      AND npiNumber IN (
          SELECT npi
          FROM healthst_master.tbl_btl_concentrate
          WHERE fk_prgID IN ({placeholders})
      )
    GROUP BY
        program_identifier,
        provider_npi,
        choozle_banner_ad_name,
        engagement_type,
        choozle_banner_link_url,
        source
    """

    try:
        conn = pymysql.connect(
            host=creds["host"],
            user=creds["user"],
            password=creds["password"],
            port=creds["port"]
        )
        with conn.cursor() as cur:
            # Use the same list of program_ids for both placeholders
            cur.execute(query, program_ids + program_ids)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=colnames)
            print(f"✅ Pulled {len(df)} Choozle banner engagements for {program_ids}")
            return df
    except Exception as e:
        print("❌ Choozle banner engagement query failed:", e)
        return pd.DataFrame()
    finally:
        conn.close()

def get_provider_specialties_for_npis(npis, creds=POSTGRES_CREDS):
    """
    Returns specialties for the given list of NPIs from dbt.int_provider_specialties.
    """
    if not npis:
        return pd.DataFrame(columns=["provider_npi", "specialty"])

    placeholders = ','.join(['%s'] * len(npis))
    query = f"""
        SELECT provider_npi, specialty
        FROM dbt.int_provider_specialties
        WHERE provider_npi IN ({placeholders})
    """

    try:
        with psycopg.connect(**creds) as conn:
            with conn.cursor() as cur:
                cur.execute(query, npis)
                rows = cur.fetchall()
                colnames = [desc.name for desc in cur.description]
                return pd.DataFrame(rows, columns=colnames)
    except Exception as e:
        print("❌ Failed to load provider specialties:", e)
        return pd.DataFrame(columns=["provider_npi", "specialty"])

def parse_psycopg_tsmultirange(multirange_obj):
    try:
        if not multirange_obj or not hasattr(multirange_obj, '__iter__'):
            return pd.Series([None, None, None])

        starts = [r.lower for r in multirange_obj if r.lower]
        ends = [r.upper for r in multirange_obj if r.upper]

        if not starts:
            return pd.Series([None, None, None])

        start_date = min(starts)
        end_date = max(ends) if any(ends) else None
        status = 'active' if any(e is None for e in ends) else 'complete'

        return pd.Series([start_date, end_date, status])

    except Exception as e:
        print(f"Error parsing range: {e}")
        return pd.Series([None, None, None])

def run_combined_engagement_query(
    program_ids,
    pg_creds=POSTGRES_CREDS,
    my_creds=MYSQL_CREDS,
    campaign_type="custom"  # "custom" or "turnkey"
):
    """
    Combines engagement sources based on campaign type.

    Parameters:
        - program_ids (list[int] or int)
        - campaign_type: "custom" includes all except AdButler, 
                         "turnkey" includes only AdButler and Email

    Returns:
        - pandas DataFrame of unified engagement data
    """
    if isinstance(program_ids, int):
        program_ids = [program_ids]

    email_df = run_email_engagement_query(program_ids, pg_creds)

    # Initialize source list
    dfs = [email_df]

    if campaign_type == "custom":
        dfs.append(run_asset_view_query(program_ids, pg_creds))
        dfs.append(run_survey_response_query(program_ids, pg_creds))
        dfs.append(run_choozle_banner_engagement_query(program_ids, my_creds))
    elif campaign_type == "turnkey":
        dfs.append(run_adbutler_banner_impression_query(program_ids, pg_creds))
    else:
        raise ValueError("campaign_type must be either 'custom' or 'turnkey'")

    combined_df = pd.concat([df for df in dfs if not df.empty], ignore_index=True)

    combined_df[['program_start_date', 'program_end_date', 'program_status']] = (
        combined_df['program_activation_ranges'].apply(parse_psycopg_tsmultirange)
    )


    combined_df[[
        'program_name',
        'program_drug_brand_name',
        'program_start_date',
        'program_end_date',
        'program_status'
    ]] = (
        combined_df
        .groupby('program_identifier')[[
            'program_name',
            'program_drug_brand_name',
            'program_start_date',
            'program_end_date',
            'program_status'
        ]]
        .transform(lambda x: x.ffill().bfill())
    )

    # Join specialty via provider_npi
    npis = combined_df['provider_npi'].dropna().unique().tolist()
    specialties_df = get_provider_specialties_for_npis(npis, pg_creds)
    if not specialties_df.empty:
        combined_df = combined_df.merge(specialties_df, on="provider_npi", how="left")
    else:
        combined_df["specialty"] = None

    columns_to_keep = [
        "program_identifier",
        "program_name",
        "program_drug_brand_name",
        "program_start_date",
        "program_end_date",
        "program_status",
        "provider_npi",
        "specialty",
        "email_campaign_identifier",
        "email_campaign_subject",
        "email_campaign_link_url",
        "choozle_banner_ad_name",
        "choozle_banner_link_url",
        "engaged_on",
        "engaged_at",
        "engagement_type",
        "source",
        "survey_question_number",
        "survey_response",
        "survey_location"
    ]

    combined_df = combined_df[[col for col in columns_to_keep if col in combined_df.columns]]

    print(f"✅ Combined {campaign_type} engagement dataset contains {len(combined_df)} rows and {len(combined_df.columns)} columns.")
    return combined_df

def run_time_spent_summary_query(program_ids, creds=MYSQL_CREDS):
    if isinstance(program_ids, int):
        program_ids = [program_ids]

    placeholders = ','.join(['%s'] * len(program_ids))

    query = f"""
    SELECT
        NPI,
        PRG_ID as program_identifier,
        start_time,
        end_time,
        AVG(mins_spent) as ATS,
        SUM(CASE WHEN (mins_spent*60) < 30 THEN 1 ELSE 0 END) / COUNT(*) AS `3-30 seconds`,
        SUM(CASE WHEN (mins_spent*60) >= 30 AND (mins_spent*60) < 60 THEN 1 ELSE 0 END) / COUNT(*) AS `30 sec -1 minute`,
        SUM(CASE WHEN (mins_spent*60) >= 60 AND (mins_spent*60) < 180 THEN 1 ELSE 0 END) / COUNT(*) AS `1 min - 3 minutes`,
        SUM(CASE WHEN (mins_spent*60) >= 180 AND (mins_spent*60) < 360 THEN 1 ELSE 0 END) / COUNT(*) AS `3 min - 6 minutes`,
        SUM(CASE WHEN (mins_spent*60) >= 360 THEN 1 ELSE 0 END) / COUNT(*) AS `> 6 min`
    FROM (
        SELECT 
            u.npiNumber AS NPI,
            a.fk_prgID AS PRG_ID,
            start_time,
            end_time,
            (TIMESTAMPDIFF(SECOND, start_time, end_time) / 60) AS mins_spent
        FROM healthst_master.tbl_app_tracking_time_spent ts
        LEFT JOIN healthst_master.tbl_user_master u ON ts.fk_uid = u.pk_uid
        JOIN healthst_master.tbl_program_assets a ON ts.asset_id = a.pk_paID
        WHERE a.fk_prgID IN ({placeholders})
          AND (TIMESTAMPDIFF(SECOND, ts.start_time, ts.end_time) / 60) BETWEEN 0.125 AND 60
    ) AS a
    WHERE LENGTH(NPI) = 10
    GROUP BY PRG_ID, NPI, start_time, end_time
    """

    try:
        conn = pymysql.connect(
            host=creds["host"],
            user=creds["user"],
            password=creds["password"],
            port=creds["port"]
        )
        with conn.cursor() as cur:
            cur.execute(query, program_ids)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=colnames)
            print(f"✅ Pulled time spent summary for {len(df)} NPI(s): {program_ids}")

            # ⏱ Print ATS in minutes and seconds
            # for _, row in df.iterrows():
            #     ats_minutes = row['ATS']
            #     minutes = int(ats_minutes)
            #     seconds = int((ats_minutes - minutes) * 60)
            #     print(f"🕒 Program {row['program_identifier']}: {minutes} min {seconds} sec average time spent")

            return df
    except Exception as e:
        print("❌ Time spent summary query failed:", e)
        return pd.DataFrame()
    finally:
        conn.close()

