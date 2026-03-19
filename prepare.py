#!/usr/bin/env python3
"""
prepare.py — Fixed pipeline file. Agent must NOT modify this.
Contains all pipeline logic:
- IV/PSI calculation (variable_analysis.py)
- Optuna 100 trials + 5-fold CV + model saving
- Top-K F1 + missed sample analysis + export
- Model artifacts (model.txt / best_params.json / feature_importance_report.xlsx)
- Result persistence (results.tsv / qc_result.json / missed_analysis.txt)
- git keep/discard

Usage: python3 prepare.py
"""

import os, sys, time, json, importlib, shutil
import numpy as np
import pandas as pd
from datetime import datetime
from multiprocessing import Pool

# ---- Dependencies ----
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import variable_analysis as va

# ---- Progress tracking (for monitor) ----
PROGRESS_FILE = os.path.join("artifacts", "progress.txt")
def _progress(step):
    try:
        os.makedirs("artifacts", exist_ok=True)
        with open(PROGRESS_FILE, 'w') as f:
            f.write(step)
    except:
        pass

# ============================================================
# Constants
# ============================================================
PARQUET_PATH = "/home/ubuntu/spark-warehouse/fdz.db/fe_wide_final_pq/"
LABEL_COL = "label"
META_COLS = ["tradnum", "cardno", "stm_dt", "data_source", "label"]
# Note: wide table has no trantime column. Query txn_label_tmp1 for trantime first.
TRAIN_FILTER = "data_source in ('train_w', 'train_b')"
TEST_FILTER = "data_source == 'test'"
IV_THRESHOLD = 0.02
OPTUNA_TRIALS = 100
N_FOLDS = 5
SEED = 42
TOPK_RANGE = (10, 1000, 10)
RESULTS_FILE = "results.tsv"
BEST_F1_FILE = "best_f1.txt"
ARTIFACT_DIR = "artifacts"
TARGET_F1 = 0.75


# ============================================================
# 1. Data Loading
# ============================================================
def load_data():
    print(f"Loading: {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH)
    df_train = df.query(TRAIN_FILTER).copy()
    df_test = df.query(TEST_FILTER).copy()
    df_train[LABEL_COL] = pd.to_numeric(df_train[LABEL_COL], errors='coerce').fillna(0).astype(int)
    print(f"Train: {len(df_train)} rows, Y=1: {df_train[LABEL_COL].sum()}")
    print(f"Test: {len(df_test)} rows")
    return df_train, df_test


# ============================================================
# 2. IV/PSI Calculation
# ============================================================
def clean_col(series):
    s = pd.to_numeric(series, errors='coerce')
    s = s.replace([np.inf, -np.inf], np.nan)
    return s

def _calc_one(args):
    var, data, target = args
    try:
        stat = va.cal_stats(data, var, target, method='dt', max_bin=5, cv_splits=3)
        if type(stat) == str:
            return [var, 0.0, None, f"cal_stats returned string: {stat}"]
        return [var, float(stat['iv'].sum()), stat, None]
    except Exception as e:
        return [var, 0.0, None, f"ERROR: {type(e).__name__}: {e}"]

def calc_iv_psi(df_train, df_test, feature_cols, n_jobs=13):
    """Full IV+PSI calculation"""
    # ---- Cleaning ----
    for col in feature_cols:
        df_train[col] = clean_col(df_train[col])

    # ---- IV ----
    print(f"\n  [IV] Computing IV for {len(feature_cols)} features with {n_jobs} workers...")
    tasks = [(col, df_train[[col, LABEL_COL]].copy(), LABEL_COL) for col in feature_cols]
    with Pool(n_jobs) as pool:
        iv_results = pool.map(_calc_one, tasks)

    iv_list, iv_tables, iv_failures = [], [], []
    for var, iv_val, stat, err in iv_results:
        iv_list.append({'feature': var, 'iv': iv_val})
        if stat is not None:
            iv_tables.append(stat)
        if err:
            iv_failures.append(f"  {var}: {err}")

    if iv_failures:
        print(f"  [WARN] {len(iv_failures)} features failed IV")

    iv_df = pd.DataFrame(iv_list).sort_values('iv', ascending=False).reset_index(drop=True)
    iv_table = pd.concat(iv_tables, ignore_index=True) if iv_tables else pd.DataFrame()

    # ---- PSI ----
    print(f"  [PSI] Computing PSI...")
    df_train_psi = df_train[feature_cols].copy()
    df_test_psi = df_test[feature_cols].copy() if len(df_test) > 0 else pd.DataFrame()

    psi_df = pd.DataFrame()
    psi_table = pd.DataFrame()

    if len(df_test_psi) > 0:
        df_train_psi['psi_label'] = 1
        df_test_psi['psi_label'] = 0
        psi_cols = [c for c in feature_cols if c in df_test_psi.columns]
        keep = psi_cols + ['psi_label']
        df_psi = pd.concat([df_train_psi[keep], df_test_psi[keep]], ignore_index=True)
        for col in psi_cols:
            df_psi[col] = clean_col(df_psi[col])

        psi_tasks = [(col, df_psi[[col, 'psi_label']].copy(), 'psi_label') for col in psi_cols]
        with Pool(n_jobs) as pool:
            psi_results = pool.map(_calc_one, psi_tasks)

        psi_list, psi_tables_list = [], []
        for var, psi_val, stat, err in psi_results:
            status = 'Stable' if psi_val < 0.1 else ('Slight' if psi_val < 0.25 else 'SIGNIFICANT')
            psi_list.append({'feature': var, 'psi': psi_val, 'status': status})
            if stat is not None:
                psi_tables_list.append(stat)

        psi_df = pd.DataFrame(psi_list).sort_values('psi', ascending=False).reset_index(drop=True)
        psi_table = pd.concat(psi_tables_list, ignore_index=True) if psi_tables_list else pd.DataFrame()
        print(f"  [PSI] Stable: {(psi_df['status']=='Stable').sum()}, Slight: {(psi_df['status']=='Slight').sum()}, SIGNIFICANT: {(psi_df['status']=='SIGNIFICANT').sum()}")

    # ---- Save Excel (4 sheets) ----
    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    excel_path = os.path.join(ARTIFACT_DIR, 'iv_psi_results.xlsx')
    with pd.ExcelWriter(excel_path) as w:
        iv_df.to_excel(w, sheet_name='iv_df', index=False)
        iv_table.to_excel(w, sheet_name='iv_table', index=False)
        psi_df.to_excel(w, sheet_name='psi_df', index=False)
        psi_table.to_excel(w, sheet_name='psi_table', index=False)
    print(f"  [IV/PSI] Saved: {excel_path}")

    # ---- Filter ----
    kept = iv_df[iv_df['iv'] >= IV_THRESHOLD]['feature'].tolist()
    print(f"  [IV] Kept {len(kept)} features (IV>={IV_THRESHOLD})")
    return kept, iv_df, psi_df


# ============================================================
# 3. Optuna + Training + Model Saving
# ============================================================
def train_model(df_train, kept_features):
    import lightgbm as lgb
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    from sklearn.model_selection import StratifiedKFold

    X = df_train[kept_features]
    y = df_train[LABEL_COL].astype(int)
    total_bad = int(y.sum())
    n_features = len(kept_features)

    skf = StratifiedKFold(n_splits=N_FOLDS, shuffle=True, random_state=SEED)

    # ---- Optuna 100 trials ----
    nl_max = min(31, max(8, total_bad // 3))
    bad_per_fold = max(1, total_bad // N_FOLDS)
    mcs_min = max(3, bad_per_fold // 4)
    mcs_max = max(mcs_min + 10, bad_per_fold)
    ff_min = 0.15 if n_features > 30 else 0.2
    ff_max = 0.5 if n_features > 30 else 0.8

    def objective(trial):
        params = {
            'objective': 'binary', 'metric': 'binary_logloss',
            'verbosity': -1, 'random_state': SEED,
            'num_leaves': trial.suggest_int('num_leaves', 6, nl_max),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
            'n_estimators': trial.suggest_int('n_estimators', 300, 1000, step=100),
            'min_child_samples': trial.suggest_int('min_child_samples', mcs_min, mcs_max),
            'max_depth': trial.suggest_int('max_depth', 3, 8),
            'feature_fraction': trial.suggest_float('feature_fraction', ff_min, ff_max),
            'bagging_fraction': trial.suggest_float('bagging_fraction', 0.6, 0.95),
            'bagging_freq': 5,
            'lambda_l1': trial.suggest_float('lambda_l1', 0.01, 1.0, log=True),
            'lambda_l2': trial.suggest_float('lambda_l2', 0.01, 1.0, log=True),
        }
        scores = []
        for tr_idx, va_idx in skf.split(X, y):
            m = lgb.LGBMClassifier(**params)
            m.fit(X.iloc[tr_idx], y.iloc[tr_idx],
                  eval_set=[(X.iloc[va_idx], y.iloc[va_idx])],
                  callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(0)])
            scores.append(m.best_score_['valid_0']['binary_logloss'])
        return np.mean(scores)

    print(f"\n  [Train] Optuna {OPTUNA_TRIALS} trials, {N_FOLDS}-fold CV...")
    _progress("8.Optuna HPO(100trials)")
    study = optuna.create_study(direction='minimize')
    study.optimize(objective, n_trials=OPTUNA_TRIALS)

    best_params = study.best_params
    best_params.update({'objective': 'binary', 'metric': 'binary_logloss',
                        'verbosity': -1, 'random_state': SEED, 'bagging_freq': 5})
    print(f"  [Train] Best logloss: {study.best_value:.6f}")

    # ---- Remove features with importance=0 ----
    _progress("9.Remove importance=0")
    print(f"  [Train] Quick fit to check feature importance...")
    quick_model = lgb.LGBMClassifier(**best_params)
    quick_model.fit(X, y)
    quick_imp = quick_model.feature_importances_
    zero_mask = quick_imp == 0
    zero_feats = [f for f, is_zero in zip(kept_features, zero_mask) if is_zero]
    if zero_feats:
        print(f"  [Train] Removing {len(zero_feats)} features with importance=0: {zero_feats}")
        kept_features = [f for f, is_zero in zip(kept_features, zero_mask) if not is_zero]
        X = df_train[kept_features]
        n_features = len(kept_features)
        print(f"  [Train] Remaining features: {n_features}")
    else:
        print(f"  [Train] All {n_features} features have importance>0, no removal needed")

    # ---- OOF CV ----
    _progress("10.OOF 5-fold CV")
    oof = np.zeros(len(X))
    importances = np.zeros(n_features)
    for fold, (tr_idx, va_idx) in enumerate(skf.split(X, y)):
        m = lgb.LGBMClassifier(**best_params)
        m.fit(X.iloc[tr_idx], y.iloc[tr_idx])
        oof[va_idx] = m.predict_proba(X.iloc[va_idx])[:, 1]
        importances += m.feature_importances_
    importances /= N_FOLDS

    # ---- Save OOF (meta_cols + features) ----
    oof_out = df_train[list(set(META_COLS) & set(df_train.columns))].copy()
    for feat in kept_features:
        oof_out[feat] = X[feat].values
    oof_out['oof_score'] = oof
    oof_path = os.path.join(ARTIFACT_DIR, 'oof_scores.csv')
    oof_out.to_csv(oof_path, index=False)
    print(f"  [Train] OOF saved: {oof_path}")

    # ---- Full training + save model ----
    final_model = lgb.LGBMClassifier(**best_params)
    final_model.fit(X, y)
    model_path = os.path.join(ARTIFACT_DIR, 'model.txt')
    final_model.booster_.save_model(model_path)
    print(f"  [Train] Model saved: {model_path}")

    # ---- best_params.json ----
    params_path = os.path.join(ARTIFACT_DIR, 'best_params.json')
    with open(params_path, 'w') as f:
        json.dump(best_params, f, indent=2)

    # ---- feature_importance_report.xlsx ----
    report = pd.DataFrame({'feature_en': kept_features, 'importance': importances})
    feat_dict_path = os.path.join(os.path.dirname(__file__), '..', 'references', 'feature_dict.csv')
    if os.path.exists(feat_dict_path):
        feat_dict = pd.read_csv(feat_dict_path)
        report = report.merge(feat_dict, on='feature_en', how='left')
    report = report.sort_values('importance', ascending=False).reset_index(drop=True)
    report_path = os.path.join(ARTIFACT_DIR, 'feature_importance_report.xlsx')
    report.to_excel(report_path, index=False)
    print(f"  [Train] Report saved: {report_path}")

    return oof, best_params, n_features, kept_features


# ============================================================
# 4. Top-K F1 + Missed Sample Analysis
# ============================================================
def evaluate_and_analyze(df_train, oof, kept_features, iv_df, psi_df):
    y = df_train[LABEL_COL].astype(int)
    total_y = int(y.sum())
    sorted_idx = np.argsort(-oof)
    sorted_y = y.values[sorted_idx]

    # ---- Top-K F1 ----
    best_f1, best_k, rows = 0.0, 0, []
    k_start, k_end, k_step = TOPK_RANGE
    for k in range(k_start, min(k_end + 1, len(sorted_y) + 1), k_step):
        top_y = int(sorted_y[:k].sum())
        p = top_y / k if k > 0 else 0
        r = top_y / total_y if total_y > 0 else 0
        f1 = 2 * p * r / (p + r) if (p + r) > 0 else 0
        rows.append({'top_k': k, 'y_count': top_y, 'precision': round(p, 4),
                     'recall': round(r, 4), 'f1': round(f1, 4),
                     'score_at_k': round(float(oof[sorted_idx[k-1]]), 6) if k <= len(oof) else 0})
        if f1 > best_f1:
            best_f1, best_k = round(f1, 4), k

    topk_df = pd.DataFrame(rows)
    topk_df.to_csv(os.path.join(ARTIFACT_DIR, 'topk_table.csv'), index=False)

    passed = best_f1 >= TARGET_F1
    print(f"\n  [QC] Best F1: {best_f1} at Top {best_k} ({'PASS' if passed else 'FAIL'})")

    # ---- Missed sample analysis ----
    diagnostics = {'best_f1': best_f1, 'best_k': best_k, 'total_y': total_y, 'total_samples': len(df_train)}

    missed_lines = []
    if not passed and best_k > 0:
        hit_mask = sorted_y[:best_k] == 1
        miss_mask = sorted_y[best_k:] == 1
        hit_count = int(hit_mask.sum())
        missed_count = int(miss_mask.sum())
        diagnostics['hit_y_at_best_k'] = hit_count
        diagnostics['missed_y'] = missed_count

        # ---- missed_positives.csv (cardno/stm_dt/rank/features) ----
        missed_indices = sorted_idx[best_k:][sorted_y[best_k:] == 1]
        missed_df = df_train.iloc[missed_indices].copy()
        missed_df['oof_score'] = oof[missed_indices]
        missed_df['rank'] = [np.where(sorted_idx == i)[0][0] + 1 for i in missed_indices]
        missed_df = missed_df.sort_values('rank')
        missed_df.to_csv(os.path.join(ARTIFACT_DIR, 'missed_positives.csv'), index=False)

        # ---- feature_gaps ----
        hit_indices = sorted_idx[:best_k][sorted_y[:best_k] == 1]
        hit_df = df_train.iloc[hit_indices]
        feature_gaps = []
        for feat in kept_features[:10]:
            if feat in df_train.columns:
                h = float(hit_df[feat].mean())
                m = float(missed_df[feat].mean())
                feature_gaps.append({'feature': feat, 'hit_mean': round(h, 4),
                                     'miss_mean': round(m, 4), 'gap': round(h - m, 4)})
        diagnostics['feature_gaps'] = feature_gaps

        # ---- missed_analysis.txt (for Todd next round) ----
        missed_lines.append(f"=== Missed Analysis (F1={best_f1} Top{best_k}) ===")
        missed_lines.append(f"Hit: {hit_count}, Missed: {missed_count}, Total Y: {total_y}")
        missed_lines.append("")
        missed_lines.append("Feature gaps (top 15 by abs gap):")
        gaps = []
        for feat in kept_features:
            if feat in df_train.columns:
                h = hit_df[feat].mean()
                m = missed_df[feat].mean()
                gaps.append((feat, h, m, h - m))
        gaps.sort(key=lambda x: abs(x[3]), reverse=True)
        for f, h, m, g in gaps[:15]:
            missed_lines.append(f"  {f:<45} hit={h:>10.4f}  miss={m:>10.4f}  gap={g:>10.4f}")
        missed_lines.append("")
        missed_lines.append("=" * 60)
        missed_lines.append("Missed samples (for PIT-compliant trace-back):")
        missed_lines.append("Note: wide table has no trantime. Query txn_label_tmp1 for trantime first!")
        missed_lines.append("")
        for _, row in missed_df.head(15).iterrows():
            cardno = row.get('cardno', '?')
            stm_dt = row.get('stm_dt', '?')
            tradnum = row.get('tradnum', '?')
            score = round(row.get('oof_score', 0), 4)
            missed_lines.append(f"  tradnum={tradnum}  cardno={cardno}  stm_dt={stm_dt}  oof={score}")
        missed_lines.append("")
        missed_lines.append("Next step:")
        missed_lines.append("  1. SSH to server")
        missed_lines.append("  2. Get trantime (PIT boundary):")
        missed_lines.append("     SELECT tradnum, trantime FROM fdz.txn_label_tmp1")
        missed_lines.append("     WHERE tradnum IN ('<tradnum1>', '<tradnum2>', ...)")
        missed_lines.append("  3. For each account (PIT-compliant):")
        missed_lines.append("     SELECT stm_dt, stm_tm, amountamt, dcflag, tranchan,")
        missed_lines.append("            fnwp2, fdfhh, termtype, is_bash, is_cross, hh")
        missed_lines.append("     FROM fdz.txn_tmp2")
        missed_lines.append("     WHERE cardno = '<cardno>' AND trantime <= <current_trantime>")
        missed_lines.append("     ORDER BY trantime DESC LIMIT 50")
        missed_lines.append("  4. Analyze behavior patterns, develop new features in train.py")

    if missed_lines:
        with open('missed_analysis.txt', 'w') as f:
            f.write('\n'.join(missed_lines))
        print(f"  [QC] Missed analysis saved: missed_analysis.txt")

    # ---- Auto-pull missed account flows (code-level enforcement) ----
    if not passed and best_k > 0:
        try:
            _progress("12.Pull Missed Flows")
            _pull_missed_account_flows(missed_df)
        except Exception as e:
            print(f"  [WARN] Failed to pull account flows: {e}")

    # ---- qc_result.json ----
    qc_result = {
        'passed': passed, 'f1_threshold': TARGET_F1, 'diagnostics': diagnostics,
        'recommendation': 'PASS' if passed else 'FAIL — need new features'
    }
    with open(os.path.join(ARTIFACT_DIR, 'qc_result.json'), 'w') as f:
        json.dump(qc_result, f, indent=2, ensure_ascii=False)

    return best_f1, best_k, passed


# ============================================================
# 4.5 Auto-pull missed account transaction flows
# ============================================================
FLOWS_FILE = "missed_account_flows.txt"
MAX_MISSED_ACCOUNTS = 10  # Max missed accounts to pull flows for

def _pull_missed_account_flows(missed_df):
    """
    Auto-pull missed account transaction flows via PySpark, write to missed_account_flows.txt.
    Todd only needs to read this file next round, no manual SSH queries needed.
    
    Steps:
    1. Get cardno and tradnum from missed_df
    2. Query txn_label_tmp1 for trantime (PIT boundary)
    3. Pull all pre-fraud flows from txn_tmp2 for each account
    4. Order by cardno, stm_dt, stm_tm ascending
    5. Write to missed_account_flows.txt
    """
    from train import get_spark
    
    print(f"\n  [Flow] Pulling transaction flows for missed accounts...")
    spark = get_spark()
    
    # Take top N missed samples
    missed_sample = missed_df.head(MAX_MISSED_ACCOUNTS)
    tradnums = missed_sample['tradnum'].astype(str).tolist()
    
    if not tradnums:
        print("  [Flow] No missed samples to trace.")
        return
    
    # Step 1: Get trantime from txn_label_tmp1
    tradnum_list = "'" + "','".join(tradnums) + "'"
    trantime_sql = f"""
        SELECT tradnum, cardno, stm_dt, trantime
        FROM fdz.txn_label_tmp1
        WHERE tradnum IN ({tradnum_list})
    """
    print(f"  [Flow] Querying trantime for {len(tradnums)} missed samples...")
    trantime_df = spark.sql(trantime_sql).toPandas()
    
    if trantime_df.empty:
        print("  [Flow] No trantime found. Skipping flow retrieval.")
        return
    
    print(f"  [Flow] Found {len(trantime_df)} samples with trantime.")
    
    # Step 2: Pull flows for each account
    flow_lines = []
    flow_lines.append("=" * 80)
    flow_lines.append("Missed Account Transaction Flows (auto-generated, PIT-compliant)")
    flow_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    flow_lines.append(f"Missed accounts: {len(trantime_df)}")
    flow_lines.append("=" * 80)
    flow_lines.append("")
    flow_lines.append("Todd: read each account flow carefully, analyze patterns, develop new features in train.py.")
    flow_lines.append("Note: all flows are pre-fraud (trantime<=current), PIT-compliant.")
    flow_lines.append("")
    
    for _, row in trantime_df.iterrows():
        cardno = str(row['cardno'])
        tradnum = str(row['tradnum'])
        stm_dt = str(row['stm_dt'])
        trantime = row['trantime']
        
        flow_lines.append("-" * 80)
        flow_lines.append(f"Account: {cardno}")
        flow_lines.append(f"Fraud txn: tradnum={tradnum}  stm_dt={stm_dt}  trantime={trantime}")
        flow_lines.append("-" * 80)
        
        # Pull all pre-fraud flows for this account, no limit
        flow_sql = f"""
            SELECT stm_dt, stm_tm, amountamt, dcflag, tranchan, 
                   fnwp2, fdfhh, termtype, is_bash, is_cross, hh
            FROM fdz.txn_tmp2
            WHERE cardno = '{cardno}' AND trantime <= {trantime}
            ORDER BY cardno, stm_dt, stm_tm
        """
        
        try:
            acct_flows = spark.sql(flow_sql).toPandas()
            
            if acct_flows.empty:
                flow_lines.append("  (no transaction flows)")
            else:
                flow_lines.append(f"  {len(acct_flows)}  transactions")
                flow_lines.append("")
                # Header
                flow_lines.append(f"  {'date':<12} {'time':<10} {'amount':>12} {'dc':>4} {'channel':<40} {'stranger':>4} {'new_cpty':>6} {'terminal':<6} {'batch':>5} {'cross':>4} {'hour':>4}")
                flow_lines.append("  " + "-" * 120)
                
                for _, tx in acct_flows.iterrows():
                    dc = 'IN' if str(tx.get('dcflag', '')) == '1' else 'OUT'
                    flow_lines.append(
                        f"  {str(tx.get('stm_dt','')):<12} "
                        f"{str(tx.get('stm_tm','')):<10} "
                        f"{str(tx.get('amountamt',''))[:12]:>12} "
                        f"{dc:>4} "
                        f"{str(tx.get('tranchan',''))[:40]:<40} "
                        f"{str(tx.get('fnwp2','')):>4} "
                        f"{str(tx.get('fdfhh','')):>6} "
                        f"{str(tx.get('termtype','')):<6} "
                        f"{str(tx.get('is_bash','')):>5} "
                        f"{str(tx.get('is_cross','')):>4} "
                        f"{str(tx.get('hh','')):>4}"
                    )
                
                # Basic stats
                flow_lines.append("")
                total_txn = len(acct_flows)
                out_txn = len(acct_flows[acct_flows['dcflag'].astype(str) == '0'])
                in_txn = total_txn - out_txn
                try:
                    amt_series = pd.to_numeric(acct_flows['amountamt'], errors='coerce')
                    total_amt = amt_series.sum()
                    out_amt = amt_series[acct_flows['dcflag'].astype(str) == '0'].sum()
                    small_100 = len(amt_series[(amt_series <= 100) & (acct_flows['dcflag'].astype(str) == '0')])
                    night_txn = len(acct_flows[acct_flows['hh'].astype(str).astype(float).fillna(12).apply(lambda h: h >= 22 or h <= 5)])
                except:
                    total_amt = out_amt = small_100 = night_txn = 0
                
                flow_lines.append(f"  Stats: total={total_txn} | out={out_txn} | in={in_txn} | "
                                  f"total_amt={total_amt:.0f} | out_amt={out_amt:.0f} | "
                                  f"small_out(<=100)={small_100} | night(22-5h)={night_txn}")
        
        except Exception as e:
            flow_lines.append(f"  [ERROR] Query failed: {e}")
        
        flow_lines.append("")
    
    # Write file
    with open(FLOWS_FILE, 'w') as f:
        f.write('\n'.join(flow_lines))
    
    print(f"  [Flow] Saved: {FLOWS_FILE} ({len(trantime_df)} accounts)")


# ============================================================
# Main
# ============================================================
def run_once():
    os.makedirs(ARTIFACT_DIR, exist_ok=True)

    # Initialize results.tsv
    if not os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, 'w') as f:
            f.write("timestamp\texp\tf1\tbest_k\tn_feat\ttime_s\tnotes\n")

    exp_id = 0
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE) as f:
            exp_id = max(0, len(f.readlines()) - 1)
    exp_id += 1

    best_ever = 0.0
    if os.path.exists(BEST_F1_FILE):
        with open(BEST_F1_FILE) as f:
            best_ever = float(f.read().strip())

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*60}")
    print(f"Experiment {exp_id} [{ts}]  (best so far: {best_ever})")
    print(f"{'='*60}")

    # ---- Load data ----
    df_train, df_test = load_data()

    # ---- Load train.py ----
    if 'train' in sys.modules:
        importlib.reload(sys.modules['train'])
    import train
    train = sys.modules['train']

    # ---- 1. Feature engineering ----
    t0 = time.time()
    _progress("4.Feature Engineering")
    print("\n  [Step 1] Feature engineering...")
    cols_before = set(df_train.columns)
    df_train = train.engineer_features(df_train.copy())
    cols_after = set(df_train.columns)
    new_cols_from_engineer = cols_after - cols_before
    available = [f for f in train.FEATURES if f in df_train.columns]
    feature_cols = [c for c in available if c not in META_COLS]
    numeric_cols = df_train[feature_cols].select_dtypes(include=[np.number]).columns.tolist()
    n_feat_total = len(numeric_cols)  # Total features before IV filter
    print(f"  Features: {n_feat_total} numeric")

    # ========== GATE: check new features (cumulative mode: warn only, no abort) ==========
    LAST_FEATURES_FILE = "last_features.txt"
    if exp_id > 1 and os.path.exists(LAST_FEATURES_FILE):
        prev_features = set()
        with open(LAST_FEATURES_FILE) as f:
            prev_features = set(line.strip() for line in f if line.strip())
        current_features = set(train.FEATURES)
        new_feature_names = current_features - prev_features

        if not new_feature_names:
            print(f"\n  [GATE WARN] No new features in FEATURES list (cumulative mode, continuing)")
            print(f"  Previous: {len(prev_features)} features, Current: {len(current_features)} features")
        else:
            # Gate 2: engineer_features() must actually create new columns
            missing_new = [f for f in new_feature_names if f not in cols_before and f not in (cols_after - cols_before)]
            if missing_new:
                print(f"\n  [GATE WARN] New features {missing_new} may not be created by engineer_features(), continuing to IV filter")
            else:
                print(f"  [GATE OK] New features: {new_feature_names}")

    # Save current FEATURES list for next round comparison
    with open(LAST_FEATURES_FILE, 'w') as f:
        for feat in train.FEATURES:
            f.write(feat + '\n')
    # ================================================================

    # ---- 2. IV/PSI ----
    _progress("5.IV/PSI Filtering")
    print("\n  [Step 2] IV/PSI (variable_analysis, method=dt, max_bin=5)...")
    df_test_feat = train.engineer_features(df_test.copy()) if len(df_test) > 0 else df_test
    kept, iv_df, psi_df = calc_iv_psi(df_train.copy(), df_test_feat, numeric_cols)
    kept = [f for f in kept if f in df_train.columns and f not in META_COLS]

    if len(kept) == 0:
        print("  [FATAL] No features pass IV threshold!")
        with open(RESULTS_FILE, 'a') as f:
            f.write(f"{ts}\t{exp_id}\t0\t0\t0\t0\tno features pass IV\n")
        return 1

    # ---- 2.5 Correlation filter (>= 0.95, keep higher IV) ----
    CORR_THRESHOLD = 0.95
    _progress("6.Correlation Filter(>=0.95)")
    print(f"\n  [Corr] Filtering features with correlation >= {CORR_THRESHOLD}...")
    corr_matrix = df_train[kept].corr().abs()
    iv_map = dict(zip(iv_df['feature'], iv_df['iv']))
    drop_corr = set()
    for i in range(len(kept)):
        for j in range(i + 1, len(kept)):
            if corr_matrix.iloc[i, j] >= CORR_THRESHOLD:
                fi, fj = kept[i], kept[j]
                if fi not in drop_corr and fj not in drop_corr:
                    # Keep higher IV, drop lower IV
                    if iv_map.get(fi, 0) >= iv_map.get(fj, 0):
                        drop_corr.add(fj)
                    else:
                        drop_corr.add(fi)
    if drop_corr:
        print(f"  [Corr] Removed {len(drop_corr)} features: {drop_corr}")
        kept = [f for f in kept if f not in drop_corr]
        print(f"  [Corr] Remaining: {len(kept)} features")
    else:
        print(f"  [Corr] No highly correlated pairs found")

    # ---- 2.8 Pre-screen with fixed params (Top69) ----
    PRE_SCREEN_TOP = 69
    if len(kept) > PRE_SCREEN_TOP:
        import lightgbm as lgb
        _progress("7.PreScreen LGB(Top69)")
        print(f"\n  [PreScreen] Fitting LightGBM with fixed params to rank {len(kept)} features...")
        pre_screen_params = {
            'objective': 'binary',
            'metric': 'binary_logloss',
            'verbosity': -1,
            'random_state': SEED,
            'n_estimators': 500,
            'max_depth': 6,
            'num_leaves': 31,
            'learning_rate': 0.05,
            'lambda_l1': 0.1,
            'lambda_l2': 0.1,
            'feature_fraction': 1.0,
            'bagging_fraction': 1.0,
            'bagging_freq': 0,
            'min_child_samples': 5,
        }
        X_pre = df_train[kept]
        y_pre = df_train[LABEL_COL].astype(int)
        pre_model = lgb.LGBMClassifier(**pre_screen_params)
        pre_model.fit(X_pre, y_pre)
        pre_imp = pre_model.feature_importances_

        # Sort by importance desc, take Top69
        imp_rank = sorted(zip(kept, pre_imp), key=lambda x: x[1], reverse=True)
        kept_top = [f for f, imp in imp_rank[:PRE_SCREEN_TOP]]
        dropped_pre = [f for f, imp in imp_rank[PRE_SCREEN_TOP:]]
        print(f"  [PreScreen] Top {PRE_SCREEN_TOP} features kept, {len(dropped_pre)} dropped")
        print(f"  [PreScreen] Lowest kept importance: {imp_rank[PRE_SCREEN_TOP-1][1]:.1f}")
        if imp_rank[PRE_SCREEN_TOP:]:
            print(f"  [PreScreen] Highest dropped: {imp_rank[PRE_SCREEN_TOP][0]} (imp={imp_rank[PRE_SCREEN_TOP][1]:.1f})")
        kept = kept_top
    else:
        print(f"\n  [PreScreen] {len(kept)} features <= {PRE_SCREEN_TOP}, skipping pre-screen")

    # ---- 3. Train ----
    print(f"\n  [Step 3] Training (Optuna {OPTUNA_TRIALS} trials)...")
    oof, best_params, n_features, kept_features = train_model(df_train, kept)

    # ---- 4. Quality Check ----
    _progress("11.Top-K F1 Eval")
    print("\n  [Step 4] Quality check...")
    f1, best_k, passed = evaluate_and_analyze(df_train, oof, kept_features, iv_df, psi_df)

    elapsed = round(time.time() - t0, 1)

    # ---- Results ----
    improved = f1 > best_ever
    if improved:
        with open(BEST_F1_FILE, 'w') as f:
            f.write(str(f1))

    notes = f"{n_features}/{n_feat_total}feat optuna{OPTUNA_TRIALS} lr={best_params.get('learning_rate',0):.4f}"
    _progress("13.Write Results/Git")
    with open(RESULTS_FILE, 'a') as f:
        f.write(f"{ts}\t{exp_id}\t{f1}\t{best_k}\t{n_features}\t{elapsed}\t{notes}\n")

    print(f"\n--- RESULT ---")
    print(f"F1:       {f1}")
    print(f"best_k:   {best_k}")
    print(f"features: {n_features}")
    print(f"time:     {elapsed}s")
    print(f"previous: {best_ever}")
    print(f"verdict:  {'>>> KEEP' if improved else '<<< DISCARD'}")
    if passed:
        print(f"*** TARGET REACHED: F1={f1} >= {TARGET_F1} ***")
    print(f"--------------")
    print(f"\nArtifacts: {ARTIFACT_DIR}/")
    print(f"  model.txt, best_params.json, feature_importance_report.xlsx")
    print(f"  iv_psi_results.xlsx, oof_scores.csv, topk_table.csv, qc_result.json")
    if not passed:
        print(f"  missed_positives.csv, missed_analysis.txt, missed_account_flows.txt")

    return 0 if passed else 1

if __name__ == '__main__':
    sys.exit(run_once())
