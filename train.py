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
    # autoresearch round 40: out/in ratio + rapid ATM burst + same-amt repeats + EDC prop + CLG transfer
    'f150d_out_in_cnt_ratio_r40', 'f150d_out_in_amt_ratio_r40',
    'f14d_atm_rapid_burst_cnt_r40', 'f14d_atm_rapid_burst_prop_r40',
    'f150d_same_amt_in_max_repeat_r40', 'f150d_same_amt_in_repeat_ratio_r40',
    'f150d_edc_out_cnt_r40', 'd_150d_edc_out_prop_r40',
    'f150d_clg_transfer_cnt_r40', 'd_150d_clg_out_prop_r40',
    'f14d_va_db_cnt_r40', 'd_14d_va_db_prop_r40',
    'd_150d_out_cnt_per_day_r40',
    # autoresearch iter3: self-transfer pattern + salary regularity + BASH anomaly + dormancy
    'f150d_self_transfer_cnt_r41', 'd_150d_self_transfer_prop_r41',
    'f150d_salary_asnm_cnt_r41', 'f150d_ibsme_regularity_r41',
    'd_amt_div_max_bash_150d_r41', 'f150d_bash_in_cnt_r41',
    'd_150d_bash_in_prop_r41',
    'f30d_out_cnt_r41', 'f30d_in_cnt_r41', 'd_30d_activity_ratio_r41',
    'd_30d_vs_150d_out_freq_r41',
    'f150d_atm_morning_prop_r41', 'd_150d_net_flow_sign_r41',
    'f7d_distinct_dfzh_out_r41', 'd_7d_dfzh_concentration_r41',
    # autoresearch iter4: outgoing BASH proportion + amt vs 3d inflow
    'f150d_out_bash_cnt_r42', 'd_150d_out_bash_prop_r42', 'd_amt_vs_3d_inflow_r42',
    # autoresearch iter6: inflow burst + last BASH gap (temporal anomaly)
    'd_3d_in_burst_vs_150d_r44', 'd_last_bash_in_gap_sec_r44',
    # autoresearch iter7: composite stranger inflow burst (proportion × diversity)
    'd_3d_stranger_inflow_burst_r45',
    # autoresearch round 46: total txn count + amt vs median + settlement + VA midnight + amt CV
    'f150d_total_txn_cnt_r46', 'd_amt_div_150d_median_out_r46',
    'f150d_va_midnight_cnt_r46', 'd_150d_va_midnight_prop_r46',
    'd_settlement_regularity_r46', 'f150d_settlement_cnt_r46',
    'd_amt_pct_rank_150d_r46', 'd_3d_in_surge_vs_14d_r46',
    'd_out_amt_cv_150d_r46',
    # autoresearch round 47: channel entropy + PRODUK DN + AHAH pattern + BASH asymmetry + burst
    'f150d_chan_nunique_r47', 'd_150d_produk_dn_out_prop_r47', 'd_150d_produk_dn_in_prop_r47',
    'd_150d_ahah_merchant_prop_r47', 'd_150d_atm_out_prop_r47',
    'd_150d_bash_in_out_ratio_r47', 'f150d_bash_out_cnt_r47',
    'd_150d_out_txn_per_dfzh_r47', 'd_150d_daily_peak_prop_r47',
    'd_14d_ibsme_settle_in_prop_r47', 'f14d_in_chan_nunique_r47',
    'd_14d_out_in_cnt_ratio_r47', 'd_14d_tarikan_prop_r47', 'd_14d_edc_out_prop_r47',
    'f3d_chan_nunique_r47', 'd_3d_out_txn_per_dfzh_r47', 'd_3d_out_minus_in_cnt_r47',
    # autoresearch round 51: BASH outflow rarity + balance drain + channel novelty + dormancy activation + mule pattern
    'd_150d_bash_out_prop_r51', 'd_150d_bash_out_amt_prop_r51',
    'd_3d_bash_out_concentration_r51', 'd_first_bash_out_gap_days_r51',
    'd_150d_balance_drain_ratio_r51', 'd_3d_balance_drain_ratio_r51',
    'd_amt_div_150d_total_in_r51', 'd_3d_out_chan_novelty_r51',
    'd_3d_produk_dn_out_prop_r51', 'd_3d_activation_burst_r51',
    'd_dormancy_out_gap_days_r51', 'd_dormancy_in_gap_days_r51',
    'd_7d_bash_in_dominance_r51', 'f7d_bash_in_sources_r51',
    'd_mule_pattern_score_r51',
    # autoresearch round 52: pseudo-normal detection + channel anomaly + burst patterns
    'd_amt_div_nonbash_avg_r52', 'd_bash_vs_nonbash_ratio_r52',
    'd_ibsme_in_dominance_150d_r52', 'd_1d_out_amt_burst_r52',
    'd_1d_out_cnt_burst_r52', 'd_1d_dfzh_burst_r52',
    'd_hour_deviation_from_mode_r52', 'd_amt_div_3d_inflow_r52',
    'f150d_lifetime_txn_cnt_r52', 'd_spending_chan_shift_r52',
    'd_amt_div_max_bash_out_r52',
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


    # ===== Round 40: out/in ratio + rapid ATM burst + same-amt repeats + EDC prop + CLG/VA transfer =====
    df_r40 = cached_spark_sql("""
        WITH hist150 AS (
            SELECT t0.tradnum,
                t2.dcflag, t2.amountamt, t2.tranchan, t2.trantime AS t2time,
                t2.termtype, t2.stm_dt,
                t0.trantime AS t0time
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=12960000
        ),
        cnt150 AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=0 THEN 1 ELSE 0 END) AS out_cnt_150d,
                SUM(CASE WHEN dcflag=1 THEN 1 ELSE 0 END) AS in_cnt_150d,
                SUM(CASE WHEN dcflag=0 THEN amountamt ELSE 0 END) AS out_amt_150d,
                SUM(CASE WHEN dcflag=1 THEN amountamt ELSE 0 END) AS in_amt_150d,
                SUM(CASE WHEN dcflag=0 AND (tranchan LIKE '%EDC%' OR tranchan LIKE '%PEMBELIAN%') THEN 1 ELSE 0 END) AS edc_out_cnt,
                SUM(CASE WHEN dcflag=0 AND (tranchan LIKE '%CLG%' OR tranchan LIKE '%COLL IB%' OR tranchan LIKE '%REALTIME COLL%') THEN 1 ELSE 0 END) AS clg_cnt,
                COUNT(DISTINCT CASE WHEN dcflag=0 THEN stm_dt END) AS active_days_150d
            FROM hist150
            GROUP BY tradnum
        ),
        in_amt_repeat AS (
            SELECT tradnum,
                ROUND(amountamt, 2) AS ramt,
                COUNT(*) AS repeat_cnt
            FROM hist150
            WHERE dcflag=1
            GROUP BY tradnum, ROUND(amountamt, 2)
        ),
        in_repeat_agg AS (
            SELECT tradnum,
                MAX(repeat_cnt) AS max_repeat_in,
                SUM(CASE WHEN repeat_cnt>=3 THEN repeat_cnt ELSE 0 END) AS repeat3_in_txns,
                SUM(1) AS distinct_in_amts
            FROM in_amt_repeat
            GROUP BY tradnum
        ),
        hist14 AS (
            SELECT t0.tradnum,
                t2.dcflag, t2.tranchan, t2.trantime AS t2time,
                t2.termtype
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=1209600
        ),
        atm_txns AS (
            SELECT tradnum, t2time,
                t2time - LAG(t2time) OVER (PARTITION BY tradnum ORDER BY t2time) AS gap_sec
            FROM hist14
            WHERE dcflag=0 AND (tranchan LIKE '%TARIKAN TUNAI%' OR tranchan LIKE '%TUNAI VIA ATM%')
        ),
        atm_burst AS (
            SELECT tradnum,
                SUM(CASE WHEN gap_sec IS NOT NULL AND gap_sec<=120 THEN 1 ELSE 0 END) AS rapid_atm_cnt,
                COUNT(*) AS total_atm_cnt
            FROM atm_txns
            GROUP BY tradnum
        ),
        va_14d AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=0 AND tranchan LIKE '%VIRTUAL ACCOUNT%' THEN 1 ELSE 0 END) AS va_db_cnt,
                SUM(CASE WHEN dcflag=0 THEN 1 ELSE 0 END) AS out14_cnt
            FROM hist14
            GROUP BY tradnum
        )
        SELECT t0.tradnum,
            CASE WHEN COALESCE(c.in_cnt_150d, 0)>0
                THEN c.out_cnt_150d*1.0/c.in_cnt_150d END AS f150d_out_in_cnt_ratio_r40,
            CASE WHEN COALESCE(c.in_amt_150d, 0)>0
                THEN c.out_amt_150d/c.in_amt_150d END AS f150d_out_in_amt_ratio_r40,
            COALESCE(ab.rapid_atm_cnt, 0) AS f14d_atm_rapid_burst_cnt_r40,
            CASE WHEN COALESCE(ab.total_atm_cnt, 0)>0
                THEN ab.rapid_atm_cnt*1.0/ab.total_atm_cnt END AS f14d_atm_rapid_burst_prop_r40,
            COALESCE(ir.max_repeat_in, 0) AS f150d_same_amt_in_max_repeat_r40,
            CASE WHEN COALESCE(ir.distinct_in_amts, 0)>0
                THEN ir.repeat3_in_txns*1.0/(ir.repeat3_in_txns + ir.distinct_in_amts) END AS f150d_same_amt_in_repeat_ratio_r40,
            COALESCE(c.edc_out_cnt, 0) AS f150d_edc_out_cnt_r40,
            CASE WHEN COALESCE(c.out_cnt_150d, 0)>0
                THEN c.edc_out_cnt*1.0/c.out_cnt_150d END AS d_150d_edc_out_prop_r40,
            COALESCE(c.clg_cnt, 0) AS f150d_clg_transfer_cnt_r40,
            CASE WHEN COALESCE(c.out_cnt_150d, 0)>0
                THEN c.clg_cnt*1.0/c.out_cnt_150d END AS d_150d_clg_out_prop_r40,
            COALESCE(va.va_db_cnt, 0) AS f14d_va_db_cnt_r40,
            CASE WHEN COALESCE(va.out14_cnt, 0)>0
                THEN va.va_db_cnt*1.0/va.out14_cnt END AS d_14d_va_db_prop_r40,
            CASE WHEN COALESCE(c.active_days_150d, 0)>0
                THEN c.out_cnt_150d*1.0/c.active_days_150d END AS d_150d_out_cnt_per_day_r40
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN cnt150 c ON t0.tradnum=c.tradnum
        LEFT JOIN in_repeat_agg ir ON t0.tradnum=ir.tradnum
        LEFT JOIN atm_burst ab ON t0.tradnum=ab.tradnum
        LEFT JOIN va_14d va ON t0.tradnum=va.tradnum
    """)
    df = df.merge(df_r40, on='tradnum', how='left')
    for col in ['f150d_out_in_cnt_ratio_r40','f150d_out_in_amt_ratio_r40',
                'f14d_atm_rapid_burst_cnt_r40','f14d_atm_rapid_burst_prop_r40',
                'f150d_same_amt_in_max_repeat_r40','f150d_same_amt_in_repeat_ratio_r40',
                'f150d_edc_out_cnt_r40','d_150d_edc_out_prop_r40',
                'f150d_clg_transfer_cnt_r40','d_150d_clg_out_prop_r40',
                'f14d_va_db_cnt_r40','d_14d_va_db_prop_r40',
                'd_150d_out_cnt_per_day_r40']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # ===== Iter 3: self-transfer + salary + BASH anomaly + dormancy + ATM morning + net flow =====
    df_r41 = cached_spark_sql("""
        WITH hist150 AS (
            SELECT t0.tradnum, t0.trantime AS t0time, t0.amountamt AS t0amt,
                t2.dcflag, t2.amountamt, t2.tranchan, t2.trantime AS t2time,
                t2.termtype, t2.stm_dt, t2.dfzh,
                CAST(t2.hh AS INT) AS hh_int,
                t2.is_bash, t2.is_cross
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=12960000
        ),
        agg150 AS (
            SELECT tradnum, t0amt,
                SUM(CASE WHEN dcflag=0 THEN 1 ELSE 0 END) AS out_cnt_150,
                SUM(CASE WHEN dcflag=0 AND tranchan LIKE '%KE PRODUK DN%' THEN 1 ELSE 0 END) AS self_out_cnt,
                SUM(CASE WHEN dcflag=1 AND tranchan LIKE '%PENERIMAAN ASNM%' THEN 1 ELSE 0 END) AS asnm_cnt,
                COUNT(DISTINCT CASE WHEN dcflag=1 AND tranchan LIKE '%IB-SME%' THEN stm_dt END) AS ibsme_in_days,
                COUNT(DISTINCT stm_dt) AS total_active_days,
                MAX(CASE WHEN is_bash=1 AND dcflag=1 THEN amountamt ELSE 0 END) AS max_bash_in_amt,
                SUM(CASE WHEN is_bash=1 AND dcflag=1 THEN 1 ELSE 0 END) AS bash_in_cnt,
                SUM(CASE WHEN dcflag=1 THEN 1 ELSE 0 END) AS in_cnt_150,
                SUM(CASE WHEN dcflag=0 AND (tranchan LIKE '%TARIKAN TUNAI%') AND hh_int>=6 AND hh_int<=10 THEN 1 ELSE 0 END) AS atm_morning_cnt,
                SUM(CASE WHEN dcflag=0 AND (tranchan LIKE '%TARIKAN TUNAI%') THEN 1 ELSE 0 END) AS atm_total_cnt,
                SUM(CASE WHEN dcflag=1 THEN amountamt ELSE 0 END) AS in_amt_150,
                SUM(CASE WHEN dcflag=0 THEN amountamt ELSE 0 END) AS out_amt_150
            FROM hist150
            GROUP BY tradnum, t0amt
        ),
        hist30 AS (
            SELECT t0.tradnum,
                t2.dcflag, t2.amountamt
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=2592000
        ),
        agg30 AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=0 THEN 1 ELSE 0 END) AS out_cnt_30,
                SUM(CASE WHEN dcflag=1 THEN 1 ELSE 0 END) AS in_cnt_30
            FROM hist30
            GROUP BY tradnum
        ),
        hist7d_dfzh AS (
            SELECT t0.tradnum,
                t2.dfzh
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=604800
                AND t2.dcflag=0
        ),
        agg7dfzh AS (
            SELECT tradnum,
                COUNT(DISTINCT dfzh) AS distinct_dfzh_7d,
                COUNT(*) AS out_cnt_7d
            FROM hist7d_dfzh
            GROUP BY tradnum
        )
        SELECT t0.tradnum,
            COALESCE(a.self_out_cnt, 0) AS f150d_self_transfer_cnt_r41,
            CASE WHEN COALESCE(a.out_cnt_150, 0)>0
                THEN a.self_out_cnt*1.0/a.out_cnt_150 END AS d_150d_self_transfer_prop_r41,
            COALESCE(a.asnm_cnt, 0) AS f150d_salary_asnm_cnt_r41,
            CASE WHEN COALESCE(a.total_active_days, 0)>0
                THEN a.ibsme_in_days*1.0/a.total_active_days END AS f150d_ibsme_regularity_r41,
            CASE WHEN COALESCE(a.max_bash_in_amt, 0)>0
                THEN a.t0amt/a.max_bash_in_amt END AS d_amt_div_max_bash_150d_r41,
            COALESCE(a.bash_in_cnt, 0) AS f150d_bash_in_cnt_r41,
            CASE WHEN COALESCE(a.in_cnt_150, 0)>0
                THEN a.bash_in_cnt*1.0/a.in_cnt_150 END AS d_150d_bash_in_prop_r41,
            COALESCE(b.out_cnt_30, 0) AS f30d_out_cnt_r41,
            COALESCE(b.in_cnt_30, 0) AS f30d_in_cnt_r41,
            CASE WHEN COALESCE(b.out_cnt_30, 0)+COALESCE(b.in_cnt_30, 0)>0
                THEN (b.out_cnt_30+b.in_cnt_30) END AS d_30d_activity_ratio_r41,
            CASE WHEN COALESCE(a.out_cnt_150, 0)>0 AND COALESCE(b.out_cnt_30, 0)>=0
                THEN b.out_cnt_30*5.0/a.out_cnt_150 END AS d_30d_vs_150d_out_freq_r41,
            CASE WHEN COALESCE(a.atm_total_cnt, 0)>0
                THEN a.atm_morning_cnt*1.0/a.atm_total_cnt END AS f150d_atm_morning_prop_r41,
            CASE WHEN (COALESCE(a.in_amt_150,0)+COALESCE(a.out_amt_150,0))>0
                THEN (a.in_amt_150 - a.out_amt_150)/(a.in_amt_150 + a.out_amt_150) END AS d_150d_net_flow_sign_r41,
            COALESCE(c.distinct_dfzh_7d, 0) AS f7d_distinct_dfzh_out_r41,
            CASE WHEN COALESCE(c.out_cnt_7d, 0)>0
                THEN c.distinct_dfzh_7d*1.0/c.out_cnt_7d END AS d_7d_dfzh_concentration_r41
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN agg150 a ON t0.tradnum=a.tradnum
        LEFT JOIN agg30 b ON t0.tradnum=b.tradnum
        LEFT JOIN agg7dfzh c ON t0.tradnum=c.tradnum
    """)
    df = df.merge(df_r41, on='tradnum', how='left')
    for col in ['f150d_self_transfer_cnt_r41','d_150d_self_transfer_prop_r41',
                'f150d_salary_asnm_cnt_r41','f150d_ibsme_regularity_r41',
                'd_amt_div_max_bash_150d_r41','f150d_bash_in_cnt_r41',
                'd_150d_bash_in_prop_r41','f30d_out_cnt_r41','f30d_in_cnt_r41',
                'd_30d_activity_ratio_r41','d_30d_vs_150d_out_freq_r41',
                'f150d_atm_morning_prop_r41','d_150d_net_flow_sign_r41',
                'f7d_distinct_dfzh_out_r41','d_7d_dfzh_concentration_r41']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')


    # ===== Iter 5: sparse account + inflow source concentration =====
    df_r43 = cached_spark_sql("""
        WITH hist150 AS (
            SELECT t0.tradnum,
                t2.dcflag, t2.amountamt, t2.dfzh, t2.stm_dt,
                t0.trantime AS t0time, t2.trantime AS t2time
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=12960000
        ),
        daily_agg AS (
            SELECT tradnum,
                COUNT(DISTINCT stm_dt) AS active_days_150d
            FROM hist150
            GROUP BY tradnum
        ),
        in_source AS (
            SELECT tradnum, dfzh,
                SUM(amountamt) AS src_amt,
                COUNT(*) AS src_cnt
            FROM hist150
            WHERE dcflag=1 AND dfzh IS NOT NULL AND TRIM(dfzh)<>''
            GROUP BY tradnum, dfzh
        ),
        in_total AS (
            SELECT tradnum,
                SUM(src_amt) AS total_in_amt,
                SUM(src_cnt) AS total_in_cnt,
                MAX(src_amt) AS max_src_amt,
                MAX(src_cnt) AS max_src_cnt,
                COUNT(DISTINCT dfzh) AS distinct_in_sources
            FROM in_source
            GROUP BY tradnum
        )
        SELECT t0.tradnum,
            COALESCE(d.active_days_150d, 0) AS f150d_active_days_r43,
            COALESCE(i.distinct_in_sources, 0) AS f150d_distinct_in_sources_r43,
            CASE WHEN COALESCE(i.total_in_amt, 0)>0
                THEN i.max_src_amt / i.total_in_amt END AS d_150d_in_src_concentration_r43,
            CASE WHEN COALESCE(i.total_in_cnt, 0)>0
                THEN i.max_src_cnt*1.0 / i.total_in_cnt END AS d_150d_in_src_cnt_conc_r43,
            CASE WHEN COALESCE(i.distinct_in_sources,0)>0 AND COALESCE(d.active_days_150d,0)>0
                THEN i.distinct_in_sources*1.0 / d.active_days_150d END AS d_in_src_per_day_r43
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN daily_agg d ON t0.tradnum=d.tradnum
        LEFT JOIN in_total i ON t0.tradnum=i.tradnum
    """)
    df = df.merge(df_r43, on='tradnum', how='left')
    for col in ['f150d_active_days_r43','f150d_distinct_in_sources_r43',
                'd_150d_in_src_concentration_r43','d_150d_in_src_cnt_conc_r43',
                'd_in_src_per_day_r43']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # ===== Iter 6: inflow burst ratio + last BASH inflow gap (temporal anomaly) =====
    df_r44 = cached_spark_sql("""
        WITH hist AS (
            SELECT t0.tradnum, t0.trantime AS t0time,
                t2.dcflag, t2.trantime AS t2time,
                t2.tranchan
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=12960000
        ),
        in_150d AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=1 THEN 1 ELSE 0 END) AS in_cnt_150d,
                COUNT(DISTINCT CASE WHEN dcflag=1 THEN CAST(FROM_UNIXTIME(t2time) AS DATE) END) AS in_days_150d
            FROM hist
            GROUP BY tradnum
        ),
        in_3d AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=1 THEN 1 ELSE 0 END) AS in_cnt_3d
            FROM hist
            WHERE t0time - t2time <= 259200
            GROUP BY tradnum
        ),
        last_bash AS (
            SELECT tradnum,
                MAX(t2time) AS last_bash_time
            FROM hist
            WHERE dcflag=1 AND (tranchan LIKE '%BASH%' OR tranchan LIKE '%NON-CUST%')
            GROUP BY tradnum
        )
        SELECT t0.tradnum,
            CASE WHEN COALESCE(a150.in_days_150d, 0)>0
                THEN COALESCE(a3.in_cnt_3d, 0) * 1.0 / (a150.in_cnt_150d * 1.0 / a150.in_days_150d * 3)
                END AS d_3d_in_burst_vs_150d_r44,
            CASE WHEN lb.last_bash_time IS NOT NULL
                THEN t0.trantime - lb.last_bash_time
                END AS d_last_bash_in_gap_sec_r44
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN in_150d a150 ON t0.tradnum=a150.tradnum
        LEFT JOIN in_3d a3 ON t0.tradnum=a3.tradnum
        LEFT JOIN last_bash lb ON t0.tradnum=lb.tradnum
    """)
    df = df.merge(df_r44, on='tradnum', how='left')
    for col in ['d_3d_in_burst_vs_150d_r44', 'd_last_bash_in_gap_sec_r44']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # ===== Iter 7: composite stranger inflow burst (proportion × diversity) =====
    # Captures "突然收到多个陌生人大额转账" pattern:
    # (3d stranger inflow amt / 3d total inflow amt) × (3d stranger counterparties / 150d distinct counterparties)
    df_r45 = cached_spark_sql("""
        WITH in_3d AS (
            SELECT t0.tradnum,
                SUM(CASE WHEN t2.dcflag=1 AND t2.fnwp2=1 THEN t2.amountamt ELSE 0 END) AS stranger_in_amt_3d,
                SUM(CASE WHEN t2.dcflag=1 THEN t2.amountamt ELSE 0 END) AS total_in_amt_3d,
                COUNT(DISTINCT CASE WHEN t2.dcflag=1 AND t2.fnwp2=1 THEN t2.dfzh END) AS stranger_in_dfzh_3d
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=259200
            GROUP BY t0.tradnum
        ),
        all_150d AS (
            SELECT t0.tradnum,
                COUNT(DISTINCT CASE WHEN t2.dfzh IS NOT NULL AND TRIM(t2.dfzh)<>'' THEN t2.dfzh END) AS total_dfzh_150d
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=12960000
            GROUP BY t0.tradnum
        )
        SELECT t0.tradnum,
            CASE WHEN COALESCE(a.total_in_amt_3d, 0)>0 AND COALESCE(b.total_dfzh_150d, 0)>0
                THEN (a.stranger_in_amt_3d / a.total_in_amt_3d) * (a.stranger_in_dfzh_3d * 1.0 / b.total_dfzh_150d)
            END AS d_3d_stranger_inflow_burst_r45
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN in_3d a ON t0.tradnum=a.tradnum
        LEFT JOIN all_150d b ON t0.tradnum=b.tradnum
    """)
    df = df.merge(df_r45, on='tradnum', how='left')
    for col in ['d_3d_stranger_inflow_burst_r45']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')


    # ===== Round 46: total txn count + amt vs median + settlement regularity + VA midnight + amt Gini =====
    df_r46 = cached_spark_sql("""
        WITH hist150 AS (
            SELECT t0.tradnum, t0.amountamt AS t0amt,
                t2.dcflag, t2.amountamt AS t2amt, t2.tranchan,
                t2.stm_dt, t2.stm_tm,
                CAST(t2.hh AS INT) AS hh_int,
                t0.trantime AS t0time, t2.trantime AS t2time
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=12960000
        ),
        agg150 AS (
            SELECT tradnum, t0amt,
                COUNT(*) AS total_txn_cnt_150d,
                SUM(CASE WHEN dcflag=0 THEN 1 ELSE 0 END) AS out_cnt_150,
                PERCENTILE_APPROX(CASE WHEN dcflag=0 THEN t2amt END, 0.5) AS median_out_150d,
                SUM(CASE WHEN stm_tm='00:00:00' AND dcflag=0 AND tranchan LIKE '%VIRTUAL ACCOUNT%' THEN 1 ELSE 0 END) AS va_midnight_cnt_150d,
                SUM(CASE WHEN dcflag=1 AND (tranchan LIKE '%SETTLEMENT%' OR tranchan LIKE '%ASNM%' OR tranchan LIKE '%CRID%' OR tranchan LIKE '%LBLL%') THEN 1 ELSE 0 END) AS settlement_in_cnt_150d,
                COUNT(DISTINCT stm_dt) AS active_days_150d,
                SUM(CASE WHEN dcflag=0 THEN t2amt ELSE 0 END) AS out_amt_150,
                SUM(CASE WHEN dcflag=0 THEN t2amt*t2amt ELSE 0 END) AS out_amt_sq_150
            FROM hist150
            GROUP BY tradnum, t0amt
        ),
        -- amt percentile rank of current txn in 150d out history
        out_amts AS (
            SELECT tradnum, t0amt, t2amt
            FROM hist150
            WHERE dcflag=0
        ),
        pctrank AS (
            SELECT tradnum, t0amt,
                SUM(CASE WHEN t2amt <= t0amt THEN 1 ELSE 0 END)*1.0 / COUNT(*) AS amt_pct_rank_150d
            FROM out_amts
            GROUP BY tradnum, t0amt
        ),
        -- 3d inflow vs 14d avg daily inflow
        in_3d AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=1 THEN t2amt ELSE 0 END) AS in_amt_3d
            FROM hist150
            WHERE t0time - t2time <= 259200
            GROUP BY tradnum
        ),
        in_14d AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=1 THEN t2amt ELSE 0 END) AS in_amt_14d,
                COUNT(DISTINCT CASE WHEN dcflag=1 THEN stm_dt END) AS in_days_14d
            FROM hist150
            WHERE t0time - t2time <= 1209600
            GROUP BY tradnum
        )
        SELECT t0.tradnum,
            COALESCE(a.total_txn_cnt_150d, 0) AS f150d_total_txn_cnt_r46,
            CASE WHEN COALESCE(a.median_out_150d, 0)>0
                THEN a.t0amt / a.median_out_150d END AS d_amt_div_150d_median_out_r46,
            COALESCE(a.va_midnight_cnt_150d, 0) AS f150d_va_midnight_cnt_r46,
            CASE WHEN COALESCE(a.out_cnt_150, 0)>0
                THEN a.va_midnight_cnt_150d*1.0/a.out_cnt_150 END AS d_150d_va_midnight_prop_r46,
            CASE WHEN COALESCE(a.active_days_150d, 0)>0
                THEN a.settlement_in_cnt_150d*1.0/a.active_days_150d END AS d_settlement_regularity_r46,
            COALESCE(a.settlement_in_cnt_150d, 0) AS f150d_settlement_cnt_r46,
            CASE WHEN COALESCE(p.amt_pct_rank_150d, 0)>=0
                THEN p.amt_pct_rank_150d END AS d_amt_pct_rank_150d_r46,
            CASE WHEN COALESCE(i14.in_days_14d, 0)>0 AND COALESCE(i14.in_amt_14d, 0)>0
                THEN COALESCE(i3.in_amt_3d, 0) / (i14.in_amt_14d / i14.in_days_14d * 3)
                END AS d_3d_in_surge_vs_14d_r46,
            CASE WHEN COALESCE(a.out_cnt_150, 0)>1 AND COALESCE(a.out_amt_150, 0)>0
                THEN SQRT(a.out_amt_sq_150/a.out_cnt_150 - (a.out_amt_150/a.out_cnt_150)*(a.out_amt_150/a.out_cnt_150)) / (a.out_amt_150/a.out_cnt_150)
                END AS d_out_amt_cv_150d_r46
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN agg150 a ON t0.tradnum=a.tradnum
        LEFT JOIN pctrank p ON t0.tradnum=p.tradnum
        LEFT JOIN in_3d i3 ON t0.tradnum=i3.tradnum
        LEFT JOIN in_14d i14 ON t0.tradnum=i14.tradnum
    """)
    df = df.merge(df_r46, on='tradnum', how='left')
    for col in ['f150d_total_txn_cnt_r46','d_amt_div_150d_median_out_r46',
                'f150d_va_midnight_cnt_r46','d_150d_va_midnight_prop_r46',
                'd_settlement_regularity_r46','f150d_settlement_cnt_r46',
                'd_amt_pct_rank_150d_r46','d_3d_in_surge_vs_14d_r46',
                'd_out_amt_cv_150d_r46']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # ===== Round 47: Channel entropy + PRODUK DN self-transfer intensity + AHAH pattern + ATM-to-MB shift + dormancy-burst =====
    df_r47 = cached_spark_sql("""
        WITH hist AS (
            SELECT t0.tradnum, t0.amountamt AS t0amt,
                t2.dcflag, t2.amountamt AS t2amt, t2.tranchan,
                t2.stm_dt, t2.cardno, t2.dfzh, t2.fnwp2,
                CAST(t2.hh AS INT) AS hh_int,
                t0.trantime AS t0time, t2.trantime AS t2time,
                t2.is_bash, t2.is_cross
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=12960000
        ),
        chan_stats AS (
            SELECT tradnum,
                -- channel entropy (diversity) for 150d
                COUNT(DISTINCT tranchan) AS chan_nunique_150d,
                -- PRODUK DN (self-transfer between own accounts) pattern
                SUM(CASE WHEN dcflag=0 AND tranchan LIKE '%PRODUK DN%' THEN 1 ELSE 0 END) AS produk_dn_out_cnt_150d,
                SUM(CASE WHEN dcflag=0 THEN 1 ELSE 0 END) AS total_out_cnt_150d,
                SUM(CASE WHEN dcflag=1 AND tranchan LIKE '%PRODUK DN%' THEN 1 ELSE 0 END) AS produk_dn_in_cnt_150d,
                SUM(CASE WHEN dcflag=1 THEN 1 ELSE 0 END) AS total_in_cnt_150d,
                -- AHAH/EDC LAIN payment pattern (merchants)
                SUM(CASE WHEN dcflag=0 AND (tranchan LIKE '%AHAH%' OR tranchan LIKE '%EDC LAIN%') THEN 1 ELSE 0 END) AS ahah_edclain_out_cnt_150d,
                SUM(CASE WHEN dcflag=0 AND (tranchan LIKE '%AHAH%' OR tranchan LIKE '%EDC LAIN%') THEN t2amt ELSE 0 END) AS ahah_edclain_out_amt_150d,
                -- ATM withdrawal pattern
                SUM(CASE WHEN dcflag=0 AND tranchan LIKE '%ATM%' THEN 1 ELSE 0 END) AS atm_out_cnt_150d,
                SUM(CASE WHEN dcflag=0 AND tranchan LIKE '%ATM%' THEN t2amt ELSE 0 END) AS atm_out_amt_150d,
                -- BASH (cross-bank) in/out asymmetry
                SUM(CASE WHEN dcflag=1 AND is_bash=1 THEN t2amt ELSE 0 END) AS bash_in_amt_150d,
                SUM(CASE WHEN dcflag=0 AND is_bash=1 THEN t2amt ELSE 0 END) AS bash_out_amt_150d,
                SUM(CASE WHEN dcflag=0 AND is_bash=1 THEN 1 ELSE 0 END) AS bash_out_cnt_150d,
                -- unique out dfzh (counterparty diversity)
                COUNT(DISTINCT CASE WHEN dcflag=0 THEN dfzh END) AS out_dfzh_nunique_150d,
                -- daily txn concentration (max txns in single day / total txns)
                COUNT(*) AS total_cnt_150d
            FROM hist
            GROUP BY tradnum
        ),
        daily_max AS (
            SELECT tradnum, MAX(day_cnt) AS max_daily_txn_cnt
            FROM (
                SELECT tradnum, stm_dt, COUNT(*) AS day_cnt
                FROM hist
                GROUP BY tradnum, stm_dt
            ) t
            GROUP BY tradnum
        ),
        -- 14d inflow channel concentration
        in_14d AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=1 AND (tranchan LIKE '%IB-SME%' OR tranchan LIKE '%SETTLEMENT%' OR tranchan LIKE '%ASNM%') THEN t2amt ELSE 0 END) AS ibsme_settle_in_amt_14d,
                SUM(CASE WHEN dcflag=1 THEN t2amt ELSE 0 END) AS total_in_amt_14d,
                SUM(CASE WHEN dcflag=1 THEN 1 ELSE 0 END) AS total_in_cnt_14d,
                COUNT(DISTINCT CASE WHEN dcflag=1 THEN tranchan END) AS in_chan_nunique_14d,
                -- 14d out amt vs in amt ratio
                SUM(CASE WHEN dcflag=0 THEN t2amt ELSE 0 END) AS total_out_amt_14d,
                SUM(CASE WHEN dcflag=0 THEN 1 ELSE 0 END) AS total_out_cnt_14d,
                -- consecutive ATM withdrawals (same day)
                SUM(CASE WHEN dcflag=0 AND tranchan LIKE '%TARIKAN TUNAI%' THEN 1 ELSE 0 END) AS tarikan_tunai_cnt_14d,
                -- EDC purchases
                SUM(CASE WHEN dcflag=0 AND tranchan LIKE '%EDC%' THEN 1 ELSE 0 END) AS edc_out_cnt_14d
            FROM hist
            WHERE t0time - t2time <= 1209600
            GROUP BY tradnum
        ),
        -- 3d burst: all channels
        burst_3d AS (
            SELECT tradnum,
                COUNT(DISTINCT tranchan) AS chan_nunique_3d,
                SUM(CASE WHEN dcflag=0 THEN 1 ELSE 0 END) AS out_cnt_3d,
                SUM(CASE WHEN dcflag=1 THEN 1 ELSE 0 END) AS in_cnt_3d,
                COUNT(DISTINCT CASE WHEN dcflag=0 THEN dfzh END) AS out_dfzh_3d
            FROM hist
            WHERE t0time - t2time <= 259200
            GROUP BY tradnum
        )
        SELECT t0.tradnum,
            -- channel diversity
            COALESCE(c.chan_nunique_150d, 0) AS f150d_chan_nunique_r47,
            -- PRODUK DN proportion
            CASE WHEN COALESCE(c.total_out_cnt_150d, 0)>0
                THEN c.produk_dn_out_cnt_150d*1.0/c.total_out_cnt_150d END AS d_150d_produk_dn_out_prop_r47,
            CASE WHEN COALESCE(c.total_in_cnt_150d, 0)>0
                THEN c.produk_dn_in_cnt_150d*1.0/c.total_in_cnt_150d END AS d_150d_produk_dn_in_prop_r47,
            -- AHAH/merchant payment proportion
            CASE WHEN COALESCE(c.total_out_cnt_150d, 0)>0
                THEN c.ahah_edclain_out_cnt_150d*1.0/c.total_out_cnt_150d END AS d_150d_ahah_merchant_prop_r47,
            -- ATM withdrawal proportion
            CASE WHEN COALESCE(c.total_out_cnt_150d, 0)>0
                THEN c.atm_out_cnt_150d*1.0/c.total_out_cnt_150d END AS d_150d_atm_out_prop_r47,
            -- BASH in vs out asymmetry
            CASE WHEN COALESCE(c.bash_out_amt_150d, 0)>0
                THEN c.bash_in_amt_150d / c.bash_out_amt_150d END AS d_150d_bash_in_out_ratio_r47,
            COALESCE(c.bash_out_cnt_150d, 0) AS f150d_bash_out_cnt_r47,
            -- out counterparty concentration
            CASE WHEN COALESCE(c.out_dfzh_nunique_150d, 0)>0
                THEN c.total_out_cnt_150d*1.0/c.out_dfzh_nunique_150d END AS d_150d_out_txn_per_dfzh_r47,
            -- daily peak concentration
            CASE WHEN COALESCE(c.total_cnt_150d, 0)>0 AND dm.max_daily_txn_cnt IS NOT NULL
                THEN dm.max_daily_txn_cnt*1.0/c.total_cnt_150d END AS d_150d_daily_peak_prop_r47,
            -- 14d IB-SME/settlement inflow dominance
            CASE WHEN COALESCE(i14.total_in_amt_14d, 0)>0
                THEN i14.ibsme_settle_in_amt_14d / i14.total_in_amt_14d END AS d_14d_ibsme_settle_in_prop_r47,
            COALESCE(i14.in_chan_nunique_14d, 0) AS f14d_in_chan_nunique_r47,
            -- 14d out/in count ratio
            CASE WHEN COALESCE(i14.total_in_cnt_14d, 0)>0
                THEN i14.total_out_cnt_14d*1.0/i14.total_in_cnt_14d END AS d_14d_out_in_cnt_ratio_r47,
            -- 14d tarikan tunai (ATM cash withdrawal) proportion
            CASE WHEN COALESCE(i14.total_out_cnt_14d, 0)>0
                THEN i14.tarikan_tunai_cnt_14d*1.0/i14.total_out_cnt_14d END AS d_14d_tarikan_prop_r47,
            -- 14d EDC purchase proportion
            CASE WHEN COALESCE(i14.total_out_cnt_14d, 0)>0
                THEN i14.edc_out_cnt_14d*1.0/i14.total_out_cnt_14d END AS d_14d_edc_out_prop_r47,
            -- 3d channel burst diversity
            COALESCE(b3.chan_nunique_3d, 0) AS f3d_chan_nunique_r47,
            -- 3d out counterparty concentration
            CASE WHEN COALESCE(b3.out_cnt_3d, 0)>0 AND COALESCE(b3.out_dfzh_3d, 0)>0
                THEN b3.out_cnt_3d*1.0/b3.out_dfzh_3d END AS d_3d_out_txn_per_dfzh_r47,
            -- 3d outflow vs inflow balance
            COALESCE(b3.out_cnt_3d, 0) - COALESCE(b3.in_cnt_3d, 0) AS d_3d_out_minus_in_cnt_r47
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN chan_stats c ON t0.tradnum=c.tradnum
        LEFT JOIN daily_max dm ON t0.tradnum=dm.tradnum
        LEFT JOIN in_14d i14 ON t0.tradnum=i14.tradnum
        LEFT JOIN burst_3d b3 ON t0.tradnum=b3.tradnum
    """)
    df = df.merge(df_r47, on='tradnum', how='left')
    for col in ['f150d_chan_nunique_r47','d_150d_produk_dn_out_prop_r47','d_150d_produk_dn_in_prop_r47',
                'd_150d_ahah_merchant_prop_r47','d_150d_atm_out_prop_r47',
                'd_150d_bash_in_out_ratio_r47','f150d_bash_out_cnt_r47',
                'd_150d_out_txn_per_dfzh_r47','d_150d_daily_peak_prop_r47',
                'd_14d_ibsme_settle_in_prop_r47','f14d_in_chan_nunique_r47',
                'd_14d_out_in_cnt_ratio_r47','d_14d_tarikan_prop_r47','d_14d_edc_out_prop_r47',
                'f3d_chan_nunique_r47','d_3d_out_txn_per_dfzh_r47','d_3d_out_minus_in_cnt_r47']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')




    # ===== Round 48: amt spike vs 90d max + cash deposit pattern + AKAO business + extreme outlier + gap density =====
    df_r48 = cached_spark_sql("""
        WITH hist150 AS (
            SELECT t0.tradnum, t0.amountamt AS t0amt,
                t2.dcflag, t2.amountamt AS t2amt, t2.tranchan,
                t2.stm_dt,
                t0.trantime AS t0time, t2.trantime AS t2time
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=12960000
        ),
        out_90d AS (
            SELECT tradnum, t0amt,
                MAX(CASE WHEN dcflag=0 THEN t2amt END) AS max_out_90d,
                PERCENTILE_APPROX(CASE WHEN dcflag=0 THEN t2amt END, 0.95) AS p95_out_90d,
                COUNT(CASE WHEN dcflag=0 THEN 1 END) AS out_cnt_90d,
                COUNT(DISTINCT CASE WHEN dcflag=0 THEN stm_dt END) AS out_active_days_90d
            FROM hist150
            WHERE t0time - t2time <= 7776000
            GROUP BY tradnum, t0amt
        ),
        in_pattern AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=1 AND tranchan LIKE '%SETORAN TUNAI%' THEN 1 ELSE 0 END) AS cash_deposit_cnt_150d,
                SUM(CASE WHEN dcflag=1 THEN 1 ELSE 0 END) AS total_in_cnt_150d,
                SUM(CASE WHEN dcflag=1 AND (tranchan LIKE '%AKAO%' OR tranchan LIKE '%SAME CURRENCY%') THEN 1 ELSE 0 END) AS akao_batch_cnt_150d,
                SUM(CASE WHEN dcflag=1 AND (tranchan LIKE '%AKAO%' OR tranchan LIKE '%SAME CURRENCY%') THEN t2amt ELSE 0 END) AS akao_batch_amt_150d,
                SUM(CASE WHEN dcflag=1 THEN t2amt ELSE 0 END) AS total_in_amt_150d,
                SUM(CASE WHEN dcflag=1 AND tranchan LIKE '%SETORAN TUNAI%' THEN t2amt ELSE 0 END) AS cash_deposit_amt_150d
            FROM hist150
            GROUP BY tradnum
        ),
        out_gap AS (
            SELECT tradnum,
                t2time,
                t2time - LAG(t2time) OVER (PARTITION BY tradnum ORDER BY t2time) AS gap_sec
            FROM hist150
            WHERE dcflag=0
        ),
        gap_stats AS (
            SELECT tradnum,
                MAX(gap_sec) AS max_gap_out_14d,
                PERCENTILE_APPROX(CASE WHEN gap_sec>0 THEN gap_sec END, 0.5) AS median_gap_out
            FROM out_gap
            GROUP BY tradnum
        ),
        hist_30d AS (
            SELECT t0.tradnum,
                COUNT(DISTINCT t2.stm_dt) AS active_days_30d,
                COUNT(*) AS total_txn_30d
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=2592000
            GROUP BY t0.tradnum
        )
        SELECT t0.tradnum,
            CASE WHEN COALESCE(o90.max_out_90d, 0)>0
                THEN o90.t0amt / o90.max_out_90d END AS d_amt_div_90d_max_out_r48,
            CASE WHEN COALESCE(o90.p95_out_90d, 0)>0
                THEN o90.t0amt / o90.p95_out_90d END AS d_amt_div_90d_p95_out_r48,
            COALESCE(o90.out_cnt_90d, 0) AS f90d_out_cnt_r48,
            CASE WHEN COALESCE(o90.out_active_days_90d, 0)>0
                THEN o90.out_cnt_90d * 1.0 / o90.out_active_days_90d END AS d_90d_out_density_r48,
            COALESCE(ip.cash_deposit_cnt_150d, 0) AS f150d_cash_deposit_cnt_r48,
            CASE WHEN COALESCE(ip.total_in_cnt_150d, 0)>0
                THEN ip.cash_deposit_cnt_150d*1.0/ip.total_in_cnt_150d END AS d_150d_cash_deposit_prop_r48,
            CASE WHEN COALESCE(ip.total_in_amt_150d, 0)>0
                THEN ip.cash_deposit_amt_150d/ip.total_in_amt_150d END AS d_150d_cash_deposit_amt_prop_r48,
            COALESCE(ip.akao_batch_cnt_150d, 0) AS f150d_akao_batch_cnt_r48,
            CASE WHEN COALESCE(ip.total_in_cnt_150d, 0)>0
                THEN ip.akao_batch_cnt_150d*1.0/ip.total_in_cnt_150d END AS d_150d_akao_in_prop_r48,
            CASE WHEN COALESCE(ip.total_in_amt_150d, 0)>0
                THEN ip.akao_batch_amt_150d/ip.total_in_amt_150d END AS d_150d_akao_amt_prop_r48,
            COALESCE(gs.max_gap_out_14d, 0) AS f150d_max_gap_out_r48,
            COALESCE(gs.median_gap_out, 0) AS f150d_median_gap_out_r48,
            CASE WHEN COALESCE(h30.active_days_30d, 0)>0
                THEN h30.total_txn_30d*1.0/30 END AS d_30d_txn_daily_rate_r48,
            COALESCE(h30.active_days_30d, 0) AS f30d_active_days_r48
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN out_90d o90 ON t0.tradnum=o90.tradnum
        LEFT JOIN in_pattern ip ON t0.tradnum=ip.tradnum
        LEFT JOIN gap_stats gs ON t0.tradnum=gs.tradnum
        LEFT JOIN hist_30d h30 ON t0.tradnum=h30.tradnum
    """)
    df = df.merge(df_r48, on='tradnum', how='left')
    for col in ['d_amt_div_90d_max_out_r48','d_amt_div_90d_p95_out_r48','f90d_out_cnt_r48',
                'd_90d_out_density_r48','f150d_cash_deposit_cnt_r48','d_150d_cash_deposit_prop_r48',
                'd_150d_cash_deposit_amt_prop_r48','f150d_akao_batch_cnt_r48','d_150d_akao_in_prop_r48',
                'd_150d_akao_amt_prop_r48','f150d_max_gap_out_r48','f150d_median_gap_out_r48',
                'd_30d_txn_daily_rate_r48','f30d_active_days_r48']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')


    # ===== Iter4 (R49): smurfing uniformity + mule speed + dormancy burst + BASH-out rarity =====
    df_r49 = cached_spark_sql("""
        WITH hist150 AS (
            SELECT t0.tradnum, t0.amountamt AS t0amt,
                t0.trantime AS t0time,
                t2.dcflag, t2.amountamt AS t2amt, t2.tranchan,
                t2.stm_dt, t2.dfzh, t2.fnwp2,
                t2.trantime AS t2time,
                t2.is_bash
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=12960000
        ),
        -- Inflow amount uniformity (low CV = smurfing)
        in_stats AS (
            SELECT tradnum,
                AVG(CASE WHEN dcflag=1 THEN t2amt END) AS in_avg_150d,
                STDDEV(CASE WHEN dcflag=1 THEN t2amt END) AS in_std_150d,
                COUNT(CASE WHEN dcflag=1 THEN 1 END) AS in_cnt_150d,
                COUNT(DISTINCT CASE WHEN dcflag=1 AND dfzh IS NOT NULL AND TRIM(dfzh)<>'' THEN dfzh END) AS in_sources_150d,
                MAX(CASE WHEN dcflag=0 THEN t2amt END) AS max_out_lifetime,
                MAX(CASE WHEN dcflag=0 THEN t2time END) AS last_out_time
            FROM hist150
            GROUP BY tradnum
        ),
        -- 90d median out for amt comparison
        out_90d AS (
            SELECT tradnum,
                PERCENTILE_APPROX(CASE WHEN dcflag=0 THEN t2amt END, 0.5) AS median_out_90d
            FROM hist150
            WHERE t0time - t2time <= 7776000
            GROUP BY tradnum
        ),
        -- 7d and 3d distinct inflow sources (mule detection)
        in_7d AS (
            SELECT tradnum,
                COUNT(DISTINCT CASE WHEN dcflag=1 AND dfzh IS NOT NULL AND TRIM(dfzh)<>'' THEN dfzh END) AS in_sources_7d,
                COUNT(CASE WHEN dcflag=1 THEN 1 END) AS in_cnt_7d
            FROM hist150
            WHERE t0time - t2time <= 604800
            GROUP BY tradnum
        ),
        in_3d AS (
            SELECT tradnum,
                COUNT(DISTINCT CASE WHEN dcflag=1 AND dfzh IS NOT NULL AND TRIM(dfzh)<>'' THEN dfzh END) AS in_sources_3d,
                COUNT(CASE WHEN dcflag=1 THEN 1 END) AS in_cnt_3d
            FROM hist150
            WHERE t0time - t2time <= 259200
            GROUP BY tradnum
        ),
        -- BASH outflow rarity
        bash_out AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=0 AND (is_bash=1 OR tranchan LIKE '%BASH%' OR tranchan LIKE '%NON-CUST%' OR tranchan LIKE '%CLG%' OR tranchan LIKE '%BANK LAIN%') THEN 1 ELSE 0 END) AS bash_out_cnt_150d,
                SUM(CASE WHEN dcflag=0 THEN 1 ELSE 0 END) AS total_out_cnt_150d
            FROM hist150
            GROUP BY tradnum
        ),
        -- Dormancy: days since last outflow (before current event)
        dormancy AS (
            SELECT tradnum, t0time,
                MAX(CASE WHEN dcflag=0 THEN t2time END) AS last_out_time_150d,
                COUNT(DISTINCT CASE WHEN dcflag=0 THEN stm_dt END) AS out_active_days_150d,
                COUNT(DISTINCT stm_dt) AS total_active_days_150d
            FROM hist150
            GROUP BY tradnum, t0time
        )
        SELECT t0.tradnum,
            -- Inflow uniformity (CV): low = smurfing
            CASE WHEN COALESCE(ist.in_avg_150d, 0)>0 AND ist.in_cnt_150d>=3
                THEN ist.in_std_150d / ist.in_avg_150d END AS f150d_in_amt_cv_r49,
            CASE WHEN ist.in_cnt_150d>=5 AND COALESCE(ist.in_avg_150d,0)>0
                AND (ist.in_std_150d / ist.in_avg_150d) < 0.1
                THEN 1 ELSE 0 END AS d_150d_in_uniform_flag_r49,
            -- Distinct inflow sources in 7d/3d
            COALESCE(i7.in_sources_7d, 0) AS f7d_distinct_in_sources_r49,
            COALESCE(i3.in_sources_3d, 0) AS f3d_distinct_in_sources_r49,
            -- Source burst: 3d sources vs 150d sources
            CASE WHEN COALESCE(ist.in_sources_150d, 0)>0
                THEN COALESCE(i3.in_sources_3d, 0)*1.0/ist.in_sources_150d END AS d_in_source_burst_3d_vs_150d_r49,
            -- BASH-out rarity
            COALESCE(bo.bash_out_cnt_150d, 0) AS f150d_bash_out_total_cnt_r49,
            CASE WHEN COALESCE(bo.total_out_cnt_150d, 0)>=5 AND COALESCE(bo.bash_out_cnt_150d, 0)<=1
                THEN 1 ELSE 0 END AS d_150d_bash_out_rare_r49,
            -- Amount vs lifetime max out
            CASE WHEN COALESCE(ist.max_out_lifetime, 0)>0
                THEN t0.amountamt / ist.max_out_lifetime END AS d_amt_div_lifetime_max_out_r49,
            -- Amount vs 90d median out
            CASE WHEN COALESCE(o90.median_out_90d, 0)>0
                THEN t0.amountamt / o90.median_out_90d END AS d_amt_div_90d_median_out_r49,
            -- Dormancy: gap in days since last outflow
            CASE WHEN d.last_out_time_150d IS NOT NULL
                THEN (d.t0time - d.last_out_time_150d) / 86400.0 END AS d_last_out_gap_days_r49,
            -- Dormancy burst: low out activity days vs total active days
            CASE WHEN COALESCE(d.total_active_days_150d, 0)>0
                THEN 1.0 - d.out_active_days_150d*1.0/d.total_active_days_150d END AS d_dormancy_burst_r49,
            -- 3d inflow count per distinct source (average)
            CASE WHEN COALESCE(i3.in_sources_3d, 0)>0
                THEN i3.in_cnt_3d*1.0/i3.in_sources_3d END AS f3d_in_cnt_distinct_sources_ratio_r49,
            -- Composite: uniformity × number of sources (high = many uniform inflows = smurfing)
            CASE WHEN COALESCE(ist.in_avg_150d, 0)>0 AND ist.in_cnt_150d>=3 AND COALESCE(ist.in_sources_150d, 0)>0
                THEN (1.0 - LEAST(ist.in_std_150d / ist.in_avg_150d, 1.0)) * ist.in_sources_150d
                END AS d_in_amt_uniformity_x_sources_r49
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN in_stats ist ON t0.tradnum=ist.tradnum
        LEFT JOIN out_90d o90 ON t0.tradnum=o90.tradnum
        LEFT JOIN in_7d i7 ON t0.tradnum=i7.tradnum
        LEFT JOIN in_3d i3 ON t0.tradnum=i3.tradnum
        LEFT JOIN bash_out bo ON t0.tradnum=bo.tradnum
        LEFT JOIN dormancy d ON t0.tradnum=d.tradnum
    """)
    df = df.merge(df_r49, on='tradnum', how='left')
    for col in ['f150d_in_amt_cv_r49','d_150d_in_uniform_flag_r49',
                'f7d_distinct_in_sources_r49','f3d_distinct_in_sources_r49',
                'd_in_source_burst_3d_vs_150d_r49',
                'f150d_bash_out_total_cnt_r49','d_150d_bash_out_rare_r49',
                'd_amt_div_lifetime_max_out_r49','d_amt_div_90d_median_out_r49',
                'd_last_out_gap_days_r49','d_dormancy_burst_r49',
                'f3d_in_cnt_distinct_sources_ratio_r49',
                'd_in_amt_uniformity_x_sources_r49']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')



    # ============================================================
    # autoresearch round 50: distribution-shape + temporal-concentration + counterparty-novelty
    # ============================================================
    df_r50 = cached_spark_sql("""
        WITH hist150 AS (
            SELECT t0.tradnum, t0.amountamt AS t0amt,
                t0.trantime AS t0time,
                t2.dcflag, t2.amountamt AS t2amt,
                t2.trantime AS t2time,
                t2.dfzh, t2.fnwp2, t2.tranchan,
                CAST(t2.hh AS INT) AS hh_int,
                t2.stm_dt
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime - t2.trantime > 0 AND t0.trantime - t2.trantime <= 12960000
        ),
        -- 1. Outflow amount skewness (150d): captures hidden large outflows among small daily txns
        out_skew AS (
            SELECT tradnum,
                AVG(CASE WHEN dcflag=0 THEN t2amt END) AS out_avg,
                STDDEV(CASE WHEN dcflag=0 THEN t2amt END) AS out_std,
                COUNT(CASE WHEN dcflag=0 THEN 1 END) AS out_n,
                AVG(CASE WHEN dcflag=0 THEN POW(t2amt, 3) END) AS out_cube_avg,
                AVG(CASE WHEN dcflag=0 THEN POW(t2amt, 2) END) AS out_sq_avg
            FROM hist150
            GROUP BY tradnum
        ),
        -- 2. Hour concentration (HHI) of ALL transactions (150d)
        hour_cnts AS (
            SELECT tradnum, hh_int,
                COUNT(*) AS hcnt
            FROM hist150
            WHERE hh_int IS NOT NULL
            GROUP BY tradnum, hh_int
        ),
        hour_total AS (
            SELECT tradnum, SUM(hcnt) AS total_cnt
            FROM hour_cnts
            GROUP BY tradnum
        ),
        hour_hhi AS (
            SELECT h.tradnum,
                SUM(POW(h.hcnt * 1.0 / ht.total_cnt, 2)) AS hhi_val
            FROM hour_cnts h
            JOIN hour_total ht ON h.tradnum = ht.tradnum
            GROUP BY h.tradnum
        ),
        -- 3. New inflow counterparty ratio: 14d sources not seen in 14d-150d gap
        in_14d_src AS (
            SELECT tradnum, dfzh
            FROM hist150
            WHERE dcflag = 1 AND dfzh IS NOT NULL AND TRIM(dfzh) <> ''
                AND t0time - t2time <= 1209600
            GROUP BY tradnum, dfzh
        ),
        in_gap_src AS (
            SELECT tradnum, dfzh
            FROM hist150
            WHERE dcflag = 1 AND dfzh IS NOT NULL AND TRIM(dfzh) <> ''
                AND t0time - t2time > 1209600
            GROUP BY tradnum, dfzh
        ),
        new_src AS (
            SELECT a.tradnum,
                COUNT(*) AS total_14d_src,
                SUM(CASE WHEN g.dfzh IS NULL THEN 1 ELSE 0 END) AS new_src_cnt
            FROM in_14d_src a
            LEFT JOIN in_gap_src g ON a.tradnum = g.tradnum AND a.dfzh = g.dfzh
            GROUP BY a.tradnum
        ),
        -- 4. BASH inflow concentration in recent 3d vs 150d
        bash_burst AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=1 AND (tranchan LIKE '%BASH%' OR tranchan LIKE '%NON-CUST%')
                    AND t0time - t2time <= 259200 THEN 1 ELSE 0 END) AS bash_in_3d,
                SUM(CASE WHEN dcflag=1 AND (tranchan LIKE '%BASH%' OR tranchan LIKE '%NON-CUST%')
                    THEN 1 ELSE 0 END) AS bash_in_150d
            FROM hist150
            GROUP BY tradnum
        )
        SELECT t0.tradnum,
            -- Outflow skewness: positive skew = mostly small with rare large (normal spending)
            -- near-zero/negative skew = uniform or large-heavy (suspicious transit)
            CASE WHEN os.out_std > 0 AND os.out_n >= 5
                THEN (os.out_cube_avg - 3*os.out_avg*os.out_sq_avg + 2*POW(os.out_avg,3))
                     / POW(os.out_std, 3)
                END AS d_150d_out_amt_skewness_r50,
            -- Hour HHI: high = concentrated in few hours (suspicious automation)
            COALESCE(hh.hhi_val, 0) AS d_150d_hour_hhi_r50,
            -- New inflow source ratio: high = many recent sources are unseen historically
            CASE WHEN COALESCE(ns.total_14d_src, 0) > 0
                THEN ns.new_src_cnt * 1.0 / ns.total_14d_src
                END AS d_14d_new_in_dfzh_ratio_r50,
            -- BASH inflow 3d concentration: high = sudden BASH burst
            CASE WHEN COALESCE(bb.bash_in_150d, 0) > 0
                THEN bb.bash_in_3d * 1.0 / bb.bash_in_150d
                END AS d_3d_bash_in_concentration_r50
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN out_skew os ON t0.tradnum = os.tradnum
        LEFT JOIN hour_hhi hh ON t0.tradnum = hh.tradnum
        LEFT JOIN new_src ns ON t0.tradnum = ns.tradnum
        LEFT JOIN bash_burst bb ON t0.tradnum = bb.tradnum
    """)
    df = df.merge(df_r50, on='tradnum', how='left')
    for col in ['d_150d_out_amt_skewness_r50', 'd_150d_hour_hhi_r50',
                'd_14d_new_in_dfzh_ratio_r50', 'd_3d_bash_in_concentration_r50']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')


    # ===== autoresearch round 51: first_bash_out + balance_drain + dormancy_activation + inflow_source_shift =====
    # Strategy: increase PRE_SCREEN_TOP 69→90 AND add features targeting missed accounts' unique patterns:
    # 1. First-time BASH outflow detection (acct 88800002534: only 2 BASH out of 58 total)
    # 2. Balance drain ratio (清空账户)
    # 3. Inflow source sudden shift (new source type in 3d vs 150d history)
    # 4. PRODUK DN self-transfer as ratio of total out (layering behavior)
    # 5. VA DB midnight proportion in recent 14d vs 150d
    # 6. Outflow channel "novelty" - what fraction of 3d out channels were never used in 14d-150d gap
    df_r51 = cached_spark_sql("""
        WITH hist150 AS (
            SELECT t0.tradnum, t0.amountamt AS t0amt,
                t0.trantime AS t0time,
                t2.dcflag, t2.amountamt AS t2amt, t2.tranchan,
                t2.stm_dt, t2.dfzh, t2.fnwp2,
                t2.trantime AS t2time,
                t2.is_bash, t2.is_cross,
                CAST(t2.hh AS INT) AS hh_int,
                t2.stm_tm
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=12960000
        ),
        -- 1. BASH outflow as fraction of TOTAL outflow (rare = never done it before)
        bash_out_stats AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=0 AND (is_bash=1 OR tranchan LIKE '%BASH%') THEN 1 ELSE 0 END) AS bash_out_cnt_150d,
                SUM(CASE WHEN dcflag=0 THEN 1 ELSE 0 END) AS total_out_cnt_150d,
                SUM(CASE WHEN dcflag=0 AND (is_bash=1 OR tranchan LIKE '%BASH%') THEN t2amt ELSE 0 END) AS bash_out_amt_150d,
                SUM(CASE WHEN dcflag=0 THEN t2amt ELSE 0 END) AS total_out_amt_150d,
                -- BASH in recent 3d only
                SUM(CASE WHEN dcflag=0 AND (is_bash=1 OR tranchan LIKE '%BASH%')
                    AND t0time - t2time <= 259200 THEN 1 ELSE 0 END) AS bash_out_cnt_3d,
                SUM(CASE WHEN dcflag=0 AND t0time - t2time <= 259200 THEN 1 ELSE 0 END) AS total_out_cnt_3d,
                -- check if first ever BASH outflow happened in last 3d
                MIN(CASE WHEN dcflag=0 AND (is_bash=1 OR tranchan LIKE '%BASH%') THEN t2time END) AS first_bash_out_time,
                MAX(CASE WHEN dcflag=0 AND (is_bash=1 OR tranchan LIKE '%BASH%') THEN t2time END) AS last_bash_out_time
            FROM hist150
            GROUP BY tradnum
        ),
        -- 2. Balance proxy: total inflow - total outflow in 150d (residual balance indicator)
        balance_proxy AS (
            SELECT tradnum, t0amt,
                SUM(CASE WHEN dcflag=1 THEN t2amt ELSE 0 END) AS total_in_amt_150d,
                SUM(CASE WHEN dcflag=0 THEN t2amt ELSE 0 END) AS total_out_amt_150d,
                -- 3d balance proxy
                SUM(CASE WHEN dcflag=1 AND t0time - t2time <= 259200 THEN t2amt ELSE 0 END) AS in_3d_amt,
                SUM(CASE WHEN dcflag=0 AND t0time - t2time <= 259200 THEN t2amt ELSE 0 END) AS out_3d_amt
            FROM hist150
            GROUP BY tradnum, t0amt
        ),
        -- 3. Channel novelty: fraction of 3d out channels NOT seen in 14d-150d gap
        chan_3d AS (
            SELECT tradnum, tranchan
            FROM hist150
            WHERE dcflag=0 AND t0time - t2time <= 259200
            GROUP BY tradnum, tranchan
        ),
        chan_gap AS (
            SELECT tradnum, tranchan
            FROM hist150
            WHERE dcflag=0 AND t0time - t2time > 259200 AND t0time - t2time <= 12960000
            GROUP BY tradnum, tranchan
        ),
        chan_novelty AS (
            SELECT c3.tradnum,
                COUNT(*) AS total_3d_chans,
                SUM(CASE WHEN cg.tranchan IS NULL THEN 1 ELSE 0 END) AS new_3d_chans
            FROM chan_3d c3
            LEFT JOIN chan_gap cg ON c3.tradnum=cg.tradnum AND c3.tranchan=cg.tranchan
            GROUP BY c3.tradnum
        ),
        -- 4. PRODUK DN self-transfer out in 3d as ratio of 3d out
        produk_3d AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=0 AND tranchan LIKE '%PRODUK DN%' THEN t2amt ELSE 0 END) AS produk_dn_out_3d_amt,
                SUM(CASE WHEN dcflag=0 THEN t2amt ELSE 0 END) AS out_3d_total_amt,
                SUM(CASE WHEN dcflag=0 AND tranchan LIKE '%PRODUK DN%' THEN 1 ELSE 0 END) AS produk_dn_out_3d_cnt,
                SUM(CASE WHEN dcflag=0 THEN 1 ELSE 0 END) AS out_3d_total_cnt
            FROM hist150
            WHERE t0time - t2time <= 259200
            GROUP BY tradnum
        ),
        -- 5. Dormancy activation: recent 3d txn count / 150d daily avg txn count (spike indicator)
        dormancy_act AS (
            SELECT tradnum, t0time,
                COUNT(CASE WHEN t0time - t2time <= 259200 THEN 1 END) AS txn_cnt_3d,
                COUNT(*) AS txn_cnt_150d,
                COUNT(DISTINCT stm_dt) AS active_days_150d,
                -- gap since last inflow before 3d window
                MAX(CASE WHEN dcflag=1 AND t0time - t2time > 259200 THEN t2time END) AS last_in_before_3d,
                -- gap since last outflow before 3d window
                MAX(CASE WHEN dcflag=0 AND t0time - t2time > 259200 THEN t2time END) AS last_out_before_3d
            FROM hist150
            GROUP BY tradnum, t0time
        ),
        -- 6. Inflow BASH dominance in recent 7d (mule account pattern)
        in_bash_7d AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=1 AND (is_bash=1 OR tranchan LIKE '%BASH%' OR tranchan LIKE '%NON-CUST%') THEN t2amt ELSE 0 END) AS bash_in_amt_7d,
                SUM(CASE WHEN dcflag=1 THEN t2amt ELSE 0 END) AS total_in_amt_7d,
                COUNT(DISTINCT CASE WHEN dcflag=1 AND (is_bash=1 OR tranchan LIKE '%BASH%' OR tranchan LIKE '%NON-CUST%') THEN dfzh END) AS bash_in_sources_7d
            FROM hist150
            WHERE t0time - t2time <= 604800
            GROUP BY tradnum
        )
        SELECT t0.tradnum,
            -- BASH outflow rarity: near 0 means account barely uses BASH for outflows
            CASE WHEN COALESCE(bo.total_out_cnt_150d, 0)>0
                THEN bo.bash_out_cnt_150d*1.0/bo.total_out_cnt_150d END AS d_150d_bash_out_prop_r51,
            -- BASH outflow amount concentration
            CASE WHEN COALESCE(bo.total_out_amt_150d, 0)>0
                THEN bo.bash_out_amt_150d/bo.total_out_amt_150d END AS d_150d_bash_out_amt_prop_r51,
            -- BASH outflow 3d burst: all BASH outs happened in last 3d = brand new behavior
            CASE WHEN COALESCE(bo.bash_out_cnt_150d, 0)>0
                THEN bo.bash_out_cnt_3d*1.0/bo.bash_out_cnt_150d END AS d_3d_bash_out_concentration_r51,
            -- First BASH out recency: gap between first ever BASH out and txn time
            CASE WHEN bo.first_bash_out_time IS NOT NULL
                THEN (t0.trantime - bo.first_bash_out_time) / 86400.0 END AS d_first_bash_out_gap_days_r51,
            -- Balance drain: (total_out - total_in) / total_in → positive = draining
            CASE WHEN COALESCE(bp.total_in_amt_150d, 0)>0
                THEN (bp.total_out_amt_150d - bp.total_in_amt_150d) / bp.total_in_amt_150d END AS d_150d_balance_drain_ratio_r51,
            -- 3d balance drain
            CASE WHEN COALESCE(bp.in_3d_amt, 0)>0
                THEN (bp.out_3d_amt - bp.in_3d_amt) / bp.in_3d_amt END AS d_3d_balance_drain_ratio_r51,
            -- Current txn amount vs remaining balance proxy
            CASE WHEN COALESCE(bp.total_in_amt_150d, 0)>0
                THEN bp.t0amt / bp.total_in_amt_150d END AS d_amt_div_150d_total_in_r51,
            -- Channel novelty in 3d
            CASE WHEN COALESCE(cn.total_3d_chans, 0)>0
                THEN cn.new_3d_chans*1.0/cn.total_3d_chans END AS d_3d_out_chan_novelty_r51,
            -- PRODUK DN self-transfer in 3d as proportion of 3d out (layering)
            CASE WHEN COALESCE(pd.out_3d_total_cnt, 0)>0
                THEN pd.produk_dn_out_3d_cnt*1.0/pd.out_3d_total_cnt END AS d_3d_produk_dn_out_prop_r51,
            -- Dormancy activation: txn spike in 3d relative to 150d daily average
            CASE WHEN COALESCE(da.active_days_150d, 0)>0 AND da.txn_cnt_150d>0
                THEN da.txn_cnt_3d / (da.txn_cnt_150d * 3.0 / da.active_days_150d) END AS d_3d_activation_burst_r51,
            -- Dormancy gap: days since last outflow before 3d window
            CASE WHEN da.last_out_before_3d IS NOT NULL
                THEN (da.t0time - da.last_out_before_3d) / 86400.0 END AS d_dormancy_out_gap_days_r51,
            -- Dormancy gap: days since last inflow before 3d window
            CASE WHEN da.last_in_before_3d IS NOT NULL
                THEN (da.t0time - da.last_in_before_3d) / 86400.0 END AS d_dormancy_in_gap_days_r51,
            -- BASH inflow dominance in 7d
            CASE WHEN COALESCE(ib.total_in_amt_7d, 0)>0
                THEN ib.bash_in_amt_7d / ib.total_in_amt_7d END AS d_7d_bash_in_dominance_r51,
            -- BASH inflow source diversity in 7d
            COALESCE(ib.bash_in_sources_7d, 0) AS f7d_bash_in_sources_r51,
            -- Composite: high BASH in dominance + low BASH out history = mule account
            CASE WHEN COALESCE(ib.total_in_amt_7d, 0)>0 AND COALESCE(bo.total_out_cnt_150d, 0)>0
                THEN (ib.bash_in_amt_7d / ib.total_in_amt_7d) * (1.0 - bo.bash_out_cnt_150d*1.0/bo.total_out_cnt_150d)
                END AS d_mule_pattern_score_r51
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN bash_out_stats bo ON t0.tradnum=bo.tradnum
        LEFT JOIN balance_proxy bp ON t0.tradnum=bp.tradnum
        LEFT JOIN chan_novelty cn ON t0.tradnum=cn.tradnum
        LEFT JOIN produk_3d pd ON t0.tradnum=pd.tradnum
        LEFT JOIN dormancy_act da ON t0.tradnum=da.tradnum
        LEFT JOIN in_bash_7d ib ON t0.tradnum=ib.tradnum
    """)
    df = df.merge(df_r51, on='tradnum', how='left')
    for col in ['d_150d_bash_out_prop_r51','d_150d_bash_out_amt_prop_r51',
                'd_3d_bash_out_concentration_r51','d_first_bash_out_gap_days_r51',
                'd_150d_balance_drain_ratio_r51','d_3d_balance_drain_ratio_r51',
                'd_amt_div_150d_total_in_r51','d_3d_out_chan_novelty_r51',
                'd_3d_produk_dn_out_prop_r51','d_3d_activation_burst_r51',
                'd_dormancy_out_gap_days_r51','d_dormancy_in_gap_days_r51',
                'd_7d_bash_in_dominance_r51','f7d_bash_in_sources_r51',
                'd_mule_pattern_score_r51']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')



    # ===== autoresearch round 52: pseudo-normal detection + merchant-then-fraud + BASH outflow anomaly =====
    # Targeted at missed accounts that look "normal" (high txn count, regular spending) but have
    # sudden anomalous outflows. Key patterns from missed_analysis:
    # - High 14d max amt makes current txn look small (d_amt_div_14d_max is low for missed)
    # - Very old BASH history (d_last_bash_in_gap_sec is very high for missed)
    # - No balance drain (d_3d_balance_drain_ratio near 0 for missed)
    # These accounts are "pseudo-normal" - they have genuine long transaction history
    # but suddenly get one anomalous BASH outflow.
    
    df_r52 = cached_spark_sql("""
        WITH hist150 AS (
            SELECT t0.tradnum, t0.amountamt AS t0amt,
                t0.trantime AS t0time,
                t2.dcflag, t2.amountamt AS t2amt, t2.tranchan,
                t2.stm_dt, t2.dfzh, t2.fnwp2,
                t2.trantime AS t2time,
                t2.is_bash, t2.termtype,
                CAST(t2.hh AS INT) AS hh_int,
                t2.stm_tm
            FROM fdz.txn_label_tmp1 t0
            LEFT JOIN fdz.txn_tmp2 t2 ON t0.cardno=t2.cardno
                AND t0.trantime-t2.trantime>0 AND t0.trantime-t2.trantime<=12960000
        ),
        -- 1. Channel-specific outflow anomaly: current BASH amt vs account's historical BASH out max
        bash_hist AS (
            SELECT tradnum, t0amt,
                MAX(CASE WHEN dcflag=0 AND (is_bash=1 OR tranchan LIKE '%BASH%') THEN t2amt END) AS max_bash_out_150d,
                AVG(CASE WHEN dcflag=0 AND (is_bash=1 OR tranchan LIKE '%BASH%') THEN t2amt END) AS avg_bash_out_150d,
                COUNT(CASE WHEN dcflag=0 AND (is_bash=1 OR tranchan LIKE '%BASH%') THEN 1 END) AS bash_out_total_cnt,
                -- non-BASH out patterns (daily life)
                AVG(CASE WHEN dcflag=0 AND is_bash=0 THEN t2amt END) AS avg_nonbash_out_150d,
                MAX(CASE WHEN dcflag=0 AND is_bash=0 THEN t2amt END) AS max_nonbash_out_150d,
                COUNT(CASE WHEN dcflag=0 AND is_bash=0 THEN 1 END) AS nonbash_out_cnt,
                -- inflow: IB-SME vs BASH vs other
                SUM(CASE WHEN dcflag=1 AND (tranchan LIKE '%IB-SME%' OR tranchan LIKE '%SETTLEMENT%') THEN t2amt ELSE 0 END) AS ibsme_in_amt,
                SUM(CASE WHEN dcflag=1 THEN t2amt ELSE 0 END) AS total_in_amt
            FROM hist150
            GROUP BY tradnum, t0amt
        ),
        -- 2. 1-day outflow burst: concentrate of 14d outflows in most-recent 1 day
        burst_1d AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=0 AND t0time - t2time <= 86400 THEN t2amt ELSE 0 END) AS out_amt_1d,
                SUM(CASE WHEN dcflag=0 AND t0time - t2time <= 1209600 THEN t2amt ELSE 0 END) AS out_amt_14d,
                SUM(CASE WHEN dcflag=0 AND t0time - t2time <= 86400 THEN 1 ELSE 0 END) AS out_cnt_1d,
                SUM(CASE WHEN dcflag=0 AND t0time - t2time <= 1209600 THEN 1 ELSE 0 END) AS out_cnt_14d,
                -- distinct counterparties in 1d vs 14d
                COUNT(DISTINCT CASE WHEN dcflag=0 AND t0time - t2time <= 86400 AND dfzh IS NOT NULL THEN dfzh END) AS out_dfzh_1d,
                COUNT(DISTINCT CASE WHEN dcflag=0 AND t0time - t2time <= 1209600 AND dfzh IS NOT NULL THEN dfzh END) AS out_dfzh_14d
            FROM hist150
            GROUP BY tradnum
        ),
        -- 3. Hour deviation: current txn hour vs account's modal hour (behavioral anomaly)
        hour_mode AS (
            SELECT tradnum, hh_int AS mode_hh,
                ROW_NUMBER() OVER (PARTITION BY tradnum ORDER BY COUNT(*) DESC) AS rn
            FROM hist150
            WHERE dcflag=0 AND hh_int IS NOT NULL
            GROUP BY tradnum, hh_int
        ),
        -- 4. BASH outflow amt vs recent 3d inflow (quick-in-quick-out ratio)
        quick_flow AS (
            SELECT tradnum, t0amt,
                SUM(CASE WHEN dcflag=1 AND t0time - t2time <= 259200 THEN t2amt ELSE 0 END) AS in_3d_total,
                SUM(CASE WHEN dcflag=1 AND t0time - t2time <= 259200 AND (is_bash=1 OR tranchan LIKE '%BASH%' OR tranchan LIKE '%NON-CUST%') THEN t2amt ELSE 0 END) AS bash_in_3d_amt,
                -- lifetime total txn count (proxy for "established" account)
                COUNT(*) AS lifetime_txn_cnt
            FROM hist150
            GROUP BY tradnum, t0amt
        ),
        -- 5. EDC/ATM dominance shift: 14d EDC+ATM proportion vs 150d (spending pattern shift)
        chan_shift AS (
            SELECT tradnum,
                SUM(CASE WHEN dcflag=0 AND (tranchan LIKE '%EDC%' OR tranchan LIKE '%ATM%' OR tranchan LIKE '%PEMBELIAN%' OR tranchan LIKE '%TARIKAN%') THEN 1 ELSE 0 END) AS spending_out_cnt,
                SUM(CASE WHEN dcflag=0 THEN 1 ELSE 0 END) AS out_cnt_150d_all,
                SUM(CASE WHEN dcflag=0 AND t0time-t2time<=1209600 AND (tranchan LIKE '%EDC%' OR tranchan LIKE '%ATM%' OR tranchan LIKE '%PEMBELIAN%' OR tranchan LIKE '%TARIKAN%') THEN 1 ELSE 0 END) AS spending_out_cnt_14d,
                SUM(CASE WHEN dcflag=0 AND t0time-t2time<=1209600 THEN 1 ELSE 0 END) AS out_cnt_14d_all
            FROM hist150
            GROUP BY tradnum
        )
        SELECT t0.tradnum,
            -- Channel-specific anomaly: BASH amt vs non-BASH avg (how unusual is this BASH outflow)
            CASE WHEN COALESCE(bh.avg_nonbash_out_150d, 0)>0
                THEN bh.t0amt / bh.avg_nonbash_out_150d END AS d_amt_div_nonbash_avg_r52,
            -- BASH out count vs non-BASH out count ratio (how rare is BASH for this account)
            CASE WHEN COALESCE(bh.nonbash_out_cnt, 0)>0
                THEN bh.bash_out_total_cnt*1.0 / bh.nonbash_out_cnt END AS d_bash_vs_nonbash_ratio_r52,
            -- IB-SME/settlement inflow dominance (merchant account indicator)
            CASE WHEN COALESCE(bh.total_in_amt, 0)>0
                THEN bh.ibsme_in_amt / bh.total_in_amt END AS d_ibsme_in_dominance_150d_r52,
            -- 1-day outflow amount burst ratio
            CASE WHEN COALESCE(b1.out_amt_14d, 0)>0
                THEN b1.out_amt_1d / b1.out_amt_14d END AS d_1d_out_amt_burst_r52,
            -- 1-day outflow count burst ratio
            CASE WHEN COALESCE(b1.out_cnt_14d, 0)>0
                THEN b1.out_cnt_1d*1.0 / b1.out_cnt_14d END AS d_1d_out_cnt_burst_r52,
            -- 1-day counterparty diversity vs 14d
            CASE WHEN COALESCE(b1.out_dfzh_14d, 0)>0
                THEN b1.out_dfzh_1d*1.0 / b1.out_dfzh_14d END AS d_1d_dfzh_burst_r52,
            -- Hour deviation from mode (circular distance)
            CASE WHEN hm.mode_hh IS NOT NULL
                THEN LEAST(ABS(CAST(t0.hh AS INT) - hm.mode_hh), 24 - ABS(CAST(t0.hh AS INT) - hm.mode_hh))
                END AS d_hour_deviation_from_mode_r52,
            -- Current txn amt vs 3d inflow (quick-in-quick-out)
            CASE WHEN COALESCE(qf.in_3d_total, 0)>0
                THEN qf.t0amt / qf.in_3d_total END AS d_amt_div_3d_inflow_r52,
            -- Lifetime txn count (established account flag - high count = pseudo-normal risk)
            COALESCE(qf.lifetime_txn_cnt, 0) AS f150d_lifetime_txn_cnt_r52,
            -- Spending channel shift: 150d spending prop vs 14d spending prop
            CASE WHEN COALESCE(cs.out_cnt_150d_all, 0)>0 AND COALESCE(cs.out_cnt_14d_all, 0)>0
                THEN (cs.spending_out_cnt*1.0/cs.out_cnt_150d_all) - (cs.spending_out_cnt_14d*1.0/cs.out_cnt_14d_all)
                END AS d_spending_chan_shift_r52,
            -- BASH amt vs max BASH historical out (spike detection within channel)
            CASE WHEN COALESCE(bh.max_bash_out_150d, 0)>0
                THEN bh.t0amt / bh.max_bash_out_150d END AS d_amt_div_max_bash_out_r52
        FROM fdz.txn_label_tmp1 t0
        LEFT JOIN bash_hist bh ON t0.tradnum=bh.tradnum
        LEFT JOIN burst_1d b1 ON t0.tradnum=b1.tradnum
        LEFT JOIN (SELECT * FROM hour_mode WHERE rn=1) hm ON t0.tradnum=hm.tradnum
        LEFT JOIN quick_flow qf ON t0.tradnum=qf.tradnum
        LEFT JOIN chan_shift cs ON t0.tradnum=cs.tradnum
    """)
    df = df.merge(df_r52, on='tradnum', how='left')
    for col in ['d_amt_div_nonbash_avg_r52','d_bash_vs_nonbash_ratio_r52',
                'd_ibsme_in_dominance_150d_r52','d_1d_out_amt_burst_r52',
                'd_1d_out_cnt_burst_r52','d_1d_dfzh_burst_r52',
                'd_hour_deviation_from_mode_r52','d_amt_div_3d_inflow_r52',
                'f150d_lifetime_txn_cnt_r52','d_spending_chan_shift_r52',
                'd_amt_div_max_bash_out_r52']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')


    return df
