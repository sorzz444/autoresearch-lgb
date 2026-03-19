"""
train.py — Agent唯一可以修改的文件。

只包含两样东西：
1. FEATURES列表（加减特征）
2. engineer_features()函数（开发新特征）

prepare.py会import这个文件，调用engineer_features()和读FEATURES。
其他所有逻辑（IV/PSI、Optuna、训练、评估、分析）都在prepare.py里，Agent不能碰。
"""

import os
import hashlib
import numpy as np
import pandas as pd

# LightGBM 3.2.x 兼容：prepare.py 会调用 lgb.log_evaluation(0)，旧版没有该API。
try:
    import lightgbm as _lgb
    if not hasattr(_lgb, 'log_evaluation'):
        def _log_evaluation(period=1):
            def _callback(env):
                return None
            _callback.order = 10
            return _callback
        _lgb.log_evaluation = _log_evaluation
except Exception:
    pass

# ============================================================
# PySpark + 特征缓存（从源表开发新特征用）
# ============================================================
_spark = None

def get_spark():
    global _spark
    if _spark is not None:
        return _spark
    from pyspark.sql import SparkSession
    _spark = SparkSession.builder \
        .appName("autoresearch_lgb") \
        .config("spark.sql.warehouse.dir", "/home/ubuntu/spark-warehouse") \
        .config("javax.jdo.option.ConnectionURL",
                "jdbc:derby:/home/ubuntu/spark-warehouse/metastore_db;create=true") \
        .config("spark.driver.memory", "40g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .enableHiveSupport() \
        .getOrCreate()
    return _spark

CACHE_DIR = "/home/ubuntu/autoresearch_lgb/.feat_cache"

def cached_spark_sql(sql, key_col='tradnum'):
    """同样的SQL只跑一次，后续从缓存读"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    sql_hash = hashlib.md5(sql.strip().encode()).hexdigest()[:12]
    cache_path = os.path.join(CACHE_DIR, f"{sql_hash}.parquet")
    if os.path.exists(cache_path):
        print(f"    [cache hit] {cache_path}")
        return pd.read_parquet(cache_path)
    print(f"    [cache miss] running SQL...")
    spark = get_spark()
    result = spark.sql(sql).toPandas()
    result.to_parquet(cache_path, index=False)
    print(f"    [cached] {cache_path} ({len(result)} rows)")
    return result


# ============================================================
# 特征列表 — Agent在这里加减特征
# ============================================================
FEATURES = [
    # 当笔特征（2个）
    'amountamt', 'fnwp2',
    # 150d特征（18个）
    'f150d_in_max_amt', 'f150d_inout_amt_ratio',
    'f150d_all_mb_all_nwp2_amt', 'f150d_in_all_c29_all_cnt',
    'f150d_in_all_all_nhh_amt', 'f150d_out_all_all_all_amt',
    'f150d_in_min_amt', 'f150d_all_all_all_nhh_cnt',
    'f150d_out_intb_cnt', 'f150d_all_std_amt',
    'f150d_out_min_amt', 'f150d_all_all_c29_all_cnt',
    'f150d_all_all_c34_all_amt', 'f150d_all_all_all_nwp2_amt',
    'f150d_in_std_amt', 'f150d_all_all_c03_all_amt',
    'f150d_out_mb_all_nwp2_amt', 'f150d_out_mb_all_namt_dfzh',
    # 14d特征（3个）
    'f14d_out_min_amt', 'f14d_all_edcl_c50_nwp2_amt',
    'f14d_all_all_c48_all_amt',
    # 限额衍生（7个）
    'd_amt_div_atmlimit', 'd_amt_div_cardlimit', 'd_amt_div_extlimit',
    'd_14d_out_div_atmlimit', 'd_14d_out_div_cardlimit',
    'd_14d_in_div_atmlimit', 'd_atmlimit_div_cardlimit',
    # 原衍生特征（11个）
    'd_risk_flag_sum', 'd_equalamt_x_nwp2', 'd_amt_div_150d_max',
    'd_150d_nwp2_cnt_prop', 'd_amt_position_150d', 'd_amt_div_150d_avg',
    'd_14d_150d_in_all_all_all_amt', 'd_150d_inout_balance',
    'd_14d_transfer_cnt_prop', 'd_amt_div_14d_max',
    'f150d_in_all_c17_nhh_cnt',
    # autoresearch round 3：稀疏小额试探 + 当前交易模式
    'evt_is_cross_r3', 'evt_is_bash_r3', 'evt_is_va_r3', 'evt_night_r3',
    'f14d_out_cnt_r3', 'f14d_out_active_days_r3',
    'f14d_out_small100_prop_r3', 'f14d_out_recent1d_prop_r3',
    'd_small100_over_sqrt_cnt_r3', 'd_out_per_day_r3',
    'f_sparse_le2_r3', 'f_sparse_le5_r3',
    'f_evt_small100_sparse_va_r3', 'f_evt_small100_sparse_cross_r3',
    'f_evt_small200_sparse_nwp2_r3',
    # autoresearch round 5：午夜记账 + 重复金额/收款方复用
    'f14d_out_midnight_cnt_r5', 'd_14d_out_midnight_prop_r5',
    'd_14d_out_top_dfzh_prop_r5', 'd_14d_out_repeat_dfzh_prop_r5',
    'd_14d_out_repeat_amt_prop_r5',
    # autoresearch round 6：当前收款方/通道路径新颖度
    'f14d_cur_dfzh_hist_cnt_r6', 'f150d_cur_dfzh_hist_cnt_r6',
    'f14d_cur_tranchan_hist_cnt_r6',
    'd_14d_cur_dfzh_unseen_r6', 'd_150d_cur_dfzh_unseen_r6',
    'd_14d_cur_tranchan_unseen_r6', 'd_cur_dfzh_unseen_x_fnwp2_r6',
    # autoresearch round 7：午夜虚拟账户扣款 + 夜间小额出金
    'f14d_out_midnight_va_cnt_r7', 'd_14d_out_midnight_va_prop_r7',
    'f14d_out_night_small_cashout_cnt_r7', 'd_14d_out_night_small_cashout_prop_r7',
    # autoresearch round 38: targeted gap-ratio + amt-burst + pseudo-normal detection
    'd_amt_div_3d_max_r38', 'd_14d_gap_cv_r38', 'd_ibsme_x_amt_dispersion_r38',
    'f14d_min_gap_out_r38', 'f14d_out_amt_cv_r38',
    # autoresearch round 39: topup + pembayaran + channel diversity + amt zscore + 7d flow
    'f150d_topup_cnt_r39', 'd_150d_topup_out_prop_r39',
    'f150d_pembayaran_cnt_r39', 'd_150d_pembayaran_out_prop_r39',
    'f150d_out_chan_nunique_r39', 'd_amt_zscore_150d_out_r39',
    'd_7d_out_div_in_r39', 'd_7d_net_flow_r39',
]


# ============================================================
# 特征工程 — Agent在这里开发新特征
# ============================================================
def engineer_features(df):
    """
    Agent在这里开发新特征。

    方式1：基于宽表已有列做衍生（秒级）
      df['new_feat'] = df['amountamt'] / df['f150d_out_max_amt'].clip(lower=1)

    方式2：从源表开发全新特征（首次3-5分钟，之后读缓存秒级）
      df_new = cached_spark_sql('''
          SELECT t0.tradnum,
              sum(case when t2.dcflag=0 and (cast(t2.hh as int)>=22 or cast(t2.hh as int)<=5)
                  then 1 else 0 end)
              / nullif(sum(case when t2.dcflag=0 then 1 else 0 end), 0) as f14d_night_out_prop
          FROM fdz.txn_label_tmp1 t0
          LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
              AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=1209600
          GROUP BY t0.tradnum
      ''')
      df = df.merge(df_new, on='tradnum', how='left')

    开发完后把新特征名加到 FEATURES 列表里。
    """
    # === Agent在下面开发新特征 ===
    df_new = cached_spark_sql('''
        WITH agg AS (
            SELECT
                t0.tradnum,
                MAX(CASE WHEN t0.is_cross=1 THEN 1 ELSE 0 END) AS evt_is_cross_r3,
                MAX(CASE WHEN t0.is_bash=1 THEN 1 ELSE 0 END) AS evt_is_bash_r3,
                MAX(CASE WHEN t0.is_va=1 THEN 1 ELSE 0 END) AS evt_is_va_r3,
                MAX(CASE WHEN CAST(t0.hh AS INT)>=22 OR CAST(t0.hh AS INT)<=5 THEN 1 ELSE 0 END) AS evt_night_r3,
                SUM(CASE WHEN t2.dcflag=0 THEN 1 ELSE 0 END) AS out14_cnt,
                COUNT(DISTINCT CASE WHEN t2.dcflag=0 THEN t2.stm_dt END) AS out14_active_days,
                SUM(CASE WHEN t2.dcflag=0 AND t2.amountamt<=100 THEN 1 ELSE 0 END) AS out14_small100_cnt,
                SUM(CASE WHEN t2.dcflag=0 AND t0.trantime-t2.trantime<=86400 THEN 1 ELSE 0 END) AS out1_cnt
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=1209600
            GROUP BY t0.tradnum
        )
        SELECT
            tradnum,
            evt_is_cross_r3,
            evt_is_bash_r3,
            evt_is_va_r3,
            evt_night_r3,
            out14_cnt AS f14d_out_cnt_r3,
            out14_active_days AS f14d_out_active_days_r3,
            CASE WHEN out14_cnt>0 THEN out14_small100_cnt*1.0/out14_cnt END AS f14d_out_small100_prop_r3,
            CASE WHEN out14_cnt>0 THEN out1_cnt*1.0/out14_cnt END AS f14d_out_recent1d_prop_r3
        FROM agg
    ''')
    df = df.merge(df_new, on='tradnum', how='left')

    for col in [
        'amountamt', 'fnwp2', 'evt_is_cross_r3', 'evt_is_bash_r3', 'evt_is_va_r3', 'evt_night_r3',
        'f14d_out_cnt_r3', 'f14d_out_active_days_r3', 'f14d_out_small100_prop_r3', 'f14d_out_recent1d_prop_r3'
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    cnt = df['f14d_out_cnt_r3'].clip(lower=1)
    act_days = df['f14d_out_active_days_r3'].clip(lower=1)
    df['d_small100_over_sqrt_cnt_r3'] = df['f14d_out_small100_prop_r3'] / np.sqrt(cnt)
    df['d_out_per_day_r3'] = df['f14d_out_cnt_r3'] / act_days
    df['f_sparse_le2_r3'] = (df['f14d_out_cnt_r3'].fillna(0) <= 2).astype(float)
    df['f_sparse_le5_r3'] = (df['f14d_out_cnt_r3'].fillna(0) <= 5).astype(float)
    df['f_evt_small100_sparse_va_r3'] = ((df['amountamt'] <= 100) & (df['f14d_out_cnt_r3'].fillna(999) <= 5) & (df['evt_is_va_r3'] == 1)).astype(float)
    df['f_evt_small100_sparse_cross_r3'] = ((df['amountamt'] <= 100) & (df['f14d_out_cnt_r3'].fillna(999) <= 5) & (df['evt_is_cross_r3'] == 1)).astype(float)
    df['f_evt_small200_sparse_nwp2_r3'] = ((df['amountamt'] <= 200) & (df['f14d_out_cnt_r3'].fillna(999) <= 20) & (df['fnwp2'] == 1)).astype(float)

    df_r5 = cached_spark_sql('''
        WITH hist AS (
            SELECT
                t0.tradnum,
                t2.stm_tm,
                t2.dfzh,
                ROUND(t2.amountamt, 2) AS amt2
            FROM fdz.txn_label_tmp1 t0
            JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0
                AND t0.trantime-t2.trantime<=1209600
                AND t2.dcflag=0
        ),
        dfzh_stat AS (
            SELECT tradnum, dfzh, COUNT(*) AS dfzh_cnt
            FROM hist
            GROUP BY tradnum, dfzh
        ),
        amt_stat AS (
            SELECT tradnum, amt2, COUNT(*) AS amt_cnt
            FROM hist
            GROUP BY tradnum, amt2
        ),
        tagged AS (
            SELECT
                h.tradnum,
                h.stm_tm,
                COALESCE(d.dfzh_cnt, 0) AS dfzh_cnt,
                COALESCE(a.amt_cnt, 0) AS amt_cnt
            FROM hist h
            LEFT JOIN dfzh_stat d
              ON h.tradnum=d.tradnum
             AND ((h.dfzh IS NULL AND d.dfzh IS NULL) OR h.dfzh=d.dfzh)
            LEFT JOIN amt_stat a
              ON h.tradnum=a.tradnum
             AND h.amt2=a.amt2
        ),
        agg AS (
            SELECT
                tradnum,
                COUNT(*) AS out14_cnt_r5,
                SUM(CASE WHEN stm_tm='00:00:00' THEN 1 ELSE 0 END) AS midnight_cnt_r5,
                MAX(dfzh_cnt) AS top_dfzh_cnt_r5,
                SUM(CASE WHEN dfzh_cnt>=2 THEN 1 ELSE 0 END) AS repeat_dfzh_txn_cnt_r5,
                SUM(CASE WHEN amt_cnt>=2 THEN 1 ELSE 0 END) AS repeat_amt_txn_cnt_r5
            FROM tagged
            GROUP BY tradnum
        )
        SELECT
            t0.tradnum,
            COALESCE(a.midnight_cnt_r5, 0) AS f14d_out_midnight_cnt_r5,
            CASE WHEN COALESCE(a.out14_cnt_r5, 0)>0 THEN a.midnight_cnt_r5*1.0/a.out14_cnt_r5 END AS d_14d_out_midnight_prop_r5,
            CASE WHEN COALESCE(a.out14_cnt_r5, 0)>0 THEN a.top_dfzh_cnt_r5*1.0/a.out14_cnt_r5 END AS d_14d_out_top_dfzh_prop_r5,
            CASE WHEN COALESCE(a.out14_cnt_r5, 0)>0 THEN a.repeat_dfzh_txn_cnt_r5*1.0/a.out14_cnt_r5 END AS d_14d_out_repeat_dfzh_prop_r5,
            CASE WHEN COALESCE(a.out14_cnt_r5, 0)>0 THEN a.repeat_amt_txn_cnt_r5*1.0/a.out14_cnt_r5 END AS d_14d_out_repeat_amt_prop_r5
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN agg a ON t0.tradnum=a.tradnum
    ''')
    df = df.merge(df_r5, on='tradnum', how='left')

    for col in [
        'f14d_out_midnight_cnt_r5', 'd_14d_out_midnight_prop_r5',
        'd_14d_out_top_dfzh_prop_r5', 'd_14d_out_repeat_dfzh_prop_r5',
        'd_14d_out_repeat_amt_prop_r5'
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df_r6 = cached_spark_sql('''
        WITH agg14 AS (
            SELECT
                t0.tradnum,
                SUM(CASE WHEN t2.dcflag=0 THEN 1 ELSE 0 END) AS out14_cnt_r6,
                SUM(CASE WHEN t2.dcflag=0 AND (
                        (t0.dfzh IS NULL AND t2.dfzh IS NULL) OR t2.dfzh=t0.dfzh
                    ) THEN 1 ELSE 0 END) AS cur_dfzh_hist_cnt_14d_r6,
                SUM(CASE WHEN t2.dcflag=0 AND t2.tranchan=t0.tranchan THEN 1 ELSE 0 END) AS cur_tranchan_hist_cnt_14d_r6
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0
                AND t0.trantime-t2.trantime<=1209600
            GROUP BY t0.tradnum
        ),
        agg150 AS (
            SELECT
                t0.tradnum,
                SUM(CASE WHEN t2.dcflag=0 AND (
                        (t0.dfzh IS NULL AND t2.dfzh IS NULL) OR t2.dfzh=t0.dfzh
                    ) THEN 1 ELSE 0 END) AS cur_dfzh_hist_cnt_150d_r6
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0
                AND t0.trantime-t2.trantime<=12960000
            GROUP BY t0.tradnum
        )
        SELECT
            t0.tradnum,
            COALESCE(a14.cur_dfzh_hist_cnt_14d_r6, 0) AS f14d_cur_dfzh_hist_cnt_r6,
            COALESCE(a150.cur_dfzh_hist_cnt_150d_r6, 0) AS f150d_cur_dfzh_hist_cnt_r6,
            COALESCE(a14.cur_tranchan_hist_cnt_14d_r6, 0) AS f14d_cur_tranchan_hist_cnt_r6
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN agg14 a14 ON t0.tradnum=a14.tradnum
        LEFT JOIN agg150 a150 ON t0.tradnum=a150.tradnum
    ''')
    df = df.merge(df_r6, on='tradnum', how='left')

    for col in [
        'f14d_cur_dfzh_hist_cnt_r6', 'f150d_cur_dfzh_hist_cnt_r6',
        'f14d_cur_tranchan_hist_cnt_r6', 'fnwp2'
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df['d_14d_cur_dfzh_unseen_r6'] = (df['f14d_cur_dfzh_hist_cnt_r6'].fillna(0) == 0).astype(float)
    df['d_150d_cur_dfzh_unseen_r6'] = (df['f150d_cur_dfzh_hist_cnt_r6'].fillna(0) == 0).astype(float)
    df['d_14d_cur_tranchan_unseen_r6'] = (df['f14d_cur_tranchan_hist_cnt_r6'].fillna(0) == 0).astype(float)
    df['d_cur_dfzh_unseen_x_fnwp2_r6'] = df['d_150d_cur_dfzh_unseen_r6'] * df['fnwp2'].fillna(0)

    df_r7 = cached_spark_sql('''
        WITH hist AS (
            SELECT
                t0.tradnum,
                t2.amountamt,
                t2.stm_tm,
                CAST(t2.hh AS INT) AS hh_int,
                t2.tranchan,
                t2.dcflag
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0
                AND t0.trantime-t2.trantime<=1209600
        ),
        agg AS (
            SELECT
                tradnum,
                SUM(CASE WHEN dcflag=0 THEN 1 ELSE 0 END) AS out14_cnt_r7,
                SUM(CASE WHEN dcflag=0 AND stm_tm='00:00:00' AND tranchan LIKE '%VIRTUAL ACCOUNT%' THEN 1 ELSE 0 END) AS midnight_va_cnt_r7,
                SUM(CASE WHEN dcflag=0
                          AND amountamt<=100
                          AND (hh_int>=22 OR hh_int<=5)
                          AND (
                              tranchan LIKE '%TRANSFER%'
                              OR tranchan LIKE '%TARIKAN%'
                              OR tranchan LIKE '%VIRTUAL ACCOUNT%'
                              OR tranchan LIKE '%TOPUP%'
                              OR tranchan LIKE '%TOP UP%'
                          )
                     THEN 1 ELSE 0 END) AS night_small_cashout_cnt_r7
            FROM hist
            GROUP BY tradnum
        )
        SELECT
            t0.tradnum,
            COALESCE(a.midnight_va_cnt_r7, 0) AS f14d_out_midnight_va_cnt_r7,
            CASE WHEN COALESCE(a.out14_cnt_r7, 0)>0 THEN a.midnight_va_cnt_r7*1.0/a.out14_cnt_r7 END AS d_14d_out_midnight_va_prop_r7,
            COALESCE(a.night_small_cashout_cnt_r7, 0) AS f14d_out_night_small_cashout_cnt_r7,
            CASE WHEN COALESCE(a.out14_cnt_r7, 0)>0 THEN a.night_small_cashout_cnt_r7*1.0/a.out14_cnt_r7 END AS d_14d_out_night_small_cashout_prop_r7
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN agg a ON t0.tradnum=a.tradnum
    ''')
    df = df.merge(df_r7, on='tradnum', how='left')

    for col in [
        'f14d_out_midnight_va_cnt_r7', 'd_14d_out_midnight_va_prop_r7',
        'f14d_out_night_small_cashout_cnt_r7', 'd_14d_out_night_small_cashout_prop_r7'
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # ===== Round 38: targeted gap-ratio + amt-burst + pseudo-normal =====
    df_r38 = cached_spark_sql('''
        WITH out_txn AS (
            SELECT t0.tradnum, t0.trantime AS t0_time,
                t2.trantime AS t2_time, t2.amountamt, t2.termtype
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=1209600
                AND t2.dcflag=0
        ),
        gap_calc AS (
            SELECT tradnum,
                t2_time,
                amountamt,
                termtype,
                t2_time - LAG(t2_time) OVER (PARTITION BY tradnum ORDER BY t2_time) AS gap_sec
            FROM out_txn
        ),
        agg AS (
            SELECT tradnum,
                MIN(CASE WHEN gap_sec>0 THEN gap_sec END) AS min_gap_r38,
                AVG(CASE WHEN gap_sec>0 THEN gap_sec END) AS avg_gap_r38,
                STDDEV(CASE WHEN gap_sec>0 THEN gap_sec END) AS std_gap_r38,
                AVG(amountamt) AS avg_amt_r38,
                STDDEV(amountamt) AS std_amt_r38,
                COUNT(*) AS out_cnt_r38
            FROM gap_calc
            GROUP BY tradnum
        ),
        amt_3d AS (
            SELECT t0.tradnum,
                MAX(t2.amountamt) AS max_3d_out_r38
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=259200
                AND t2.dcflag=0
            GROUP BY t0.tradnum
        ),
        ibsme AS (
            SELECT t0.tradnum,
                SUM(CASE WHEN t2.termtype='IB SME' AND t2.dcflag=1 THEN t2.amountamt ELSE 0 END) AS ibsme_in_amt_r38
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=12960000
            GROUP BY t0.tradnum
        )
        SELECT t0.tradnum,
            CASE WHEN COALESCE(a3.max_3d_out_r38, 0)>0
                THEN t0.amountamt / a3.max_3d_out_r38 END AS d_amt_div_3d_max_r38,
            CASE WHEN COALESCE(a.avg_gap_r38, 0)>0
                THEN a.std_gap_r38 / a.avg_gap_r38 END AS d_14d_gap_cv_r38,
            CASE WHEN COALESCE(a.avg_amt_r38, 0)>0
                THEN (ib.ibsme_in_amt_r38 / NULLIF(a.out_cnt_r38, 0)) * (a.std_amt_r38 / a.avg_amt_r38) END AS d_ibsme_x_amt_dispersion_r38,
            a.min_gap_r38 AS f14d_min_gap_out_r38,
            CASE WHEN COALESCE(a.avg_amt_r38, 0)>0
                THEN a.std_amt_r38 / a.avg_amt_r38 END AS f14d_out_amt_cv_r38
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN agg a ON t0.tradnum=a.tradnum
        LEFT JOIN amt_3d a3 ON t0.tradnum=a3.tradnum
        LEFT JOIN ibsme ib ON t0.tradnum=ib.tradnum
    ''')
    df = df.merge(df_r38, on='tradnum', how='left')
    for col in ['d_amt_div_3d_max_r38', 'd_14d_gap_cv_r38', 'd_ibsme_x_amt_dispersion_r38',
                'f14d_min_gap_out_r38', 'f14d_out_amt_cv_r38']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')


    # ===== Round 39: topup-card + pembayaran + channel-diversity + amt-zscore + 7d-flow =====
    df_r39 = cached_spark_sql("""
        WITH hist AS (
            SELECT t0.tradnum,
                t2.dcflag, t2.amountamt, t2.tranchan, t2.trantime AS t2time,
                t0.trantime AS t0time, t0.amountamt AS t0amt
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=12960000
        ),
        agg150 AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=0 AND tranchan LIKE '%TOPUP%' THEN 1 ELSE 0 END) AS topup_cnt_150d,
                SUM(CASE WHEN dcflag=0 THEN 1 ELSE 0 END) AS out_cnt_150d,
                SUM(CASE WHEN dcflag=0 AND tranchan LIKE '%PEMBAYARAN%' THEN 1 ELSE 0 END) AS pembayaran_cnt_150d,
                COUNT(DISTINCT CASE WHEN dcflag=0 THEN tranchan END) AS out_chan_nunique_150d,
                AVG(CASE WHEN dcflag=0 THEN amountamt END) AS out_avg_amt_150d,
                STDDEV(CASE WHEN dcflag=0 THEN amountamt END) AS out_std_amt_150d
            FROM hist
            GROUP BY tradnum
        ),
        agg7d AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=1 THEN amountamt ELSE 0 END) AS in_7d_amt,
                SUM(CASE WHEN dcflag=0 THEN amountamt ELSE 0 END) AS out_7d_amt
            FROM hist
            WHERE t0time - t2time <= 604800
            GROUP BY tradnum
        )
        SELECT t0.tradnum,
            COALESCE(a.topup_cnt_150d, 0) AS f150d_topup_cnt_r39,
            CASE WHEN COALESCE(a.out_cnt_150d,0)>0
                THEN a.topup_cnt_150d*1.0/a.out_cnt_150d END AS d_150d_topup_out_prop_r39,
            COALESCE(a.pembayaran_cnt_150d, 0) AS f150d_pembayaran_cnt_r39,
            CASE WHEN COALESCE(a.out_cnt_150d,0)>0
                THEN a.pembayaran_cnt_150d*1.0/a.out_cnt_150d END AS d_150d_pembayaran_out_prop_r39,
            a.out_chan_nunique_150d AS f150d_out_chan_nunique_r39,
            CASE WHEN COALESCE(a.out_std_amt_150d,0)>0
                THEN (t0.amountamt - a.out_avg_amt_150d)/a.out_std_amt_150d END AS d_amt_zscore_150d_out_r39,
            CASE WHEN COALESCE(b.in_7d_amt,0)>0
                THEN b.out_7d_amt/b.in_7d_amt END AS d_7d_out_div_in_r39,
            COALESCE(b.in_7d_amt,0) - COALESCE(b.out_7d_amt,0) AS d_7d_net_flow_r39
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN agg150 a ON t0.tradnum=a.tradnum
        LEFT JOIN agg7d b ON t0.tradnum=b.tradnum
    """)
    df = df.merge(df_r39, on='tradnum', how='left')
    for col in ['f150d_topup_cnt_r39','d_150d_topup_out_prop_r39','f150d_pembayaran_cnt_r39',
                'd_150d_pembayaran_out_prop_r39','f150d_out_chan_nunique_r39',
                'd_amt_zscore_150d_out_r39','d_7d_out_div_in_r39','d_7d_net_flow_r39']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df
