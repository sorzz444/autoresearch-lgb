# Autoresearch-LGB

Automated feature engineering framework for BCA Bank Indonesia anti-fraud victim model.

## Objective

Starting from 88 positive samples (fraud victims), iteratively develop features via automated AI-driven loops to improve Top-K F1 from 0.6486 to 0.75 (TP_at_150 >= 72).

## Architecture

OpenClaw (AI Agent platform) drives a `.prose` loop file. Each iteration:

```
Todd (AI Agent) analyzes transaction flows → develops new features (train.py) → prepare.py auto-trains and evaluates
```

### Core Files

| File | Description |
|------|-------------|
| `prepare.py` | Fixed pipeline: IV → Correlation → Top69 PreScreen → Optuna → OOF (Agent cannot modify, chmod 444) |
| `train.py` | Agent's only modifiable file: FEATURES list + engineer_features() |
| `variable_analysis.py` | IV/PSI calculation module |
| `autoresearch_loop.prose` | OpenProse loop orchestration file |
| `monitor_v4.sh` | 14-step real-time pipeline monitor |

### Feature Selection Pipeline (prepare.py)

```
FEATURES list (~150+) → engineer_features() creates columns
  → IV >= 0.02 filter
  → Correlation >= 0.95 filter (keep higher IV)
  → PreScreen LightGBM (500 trees, depth=6) → Top 69
  → Optuna 100 trials hyperparameter search
  → Remove importance=0 features
  → OOF 5-fold CV → Top-K F1 evaluation
```

### Key Design Decisions

- **No sample imbalance handling**: Never use is_unbalance / scale_pos_weight / class_weight — empirically proven to significantly degrade performance
- **Cumulative mode**: train.py never reverts; features only accumulate; GATE warns but doesn't abort
- **Auto-pull flows**: prepare.py automatically queries missed accounts' full transaction sequences via PySpark
- **Evaluation metric**: Top-K F1 (not AUC/standard F1); final delivery target is TP_at_150 >= 72

### references/

Historical project rule SQL and feature engineering reference materials. Todd reads these each iteration for domain knowledge.

## Experiment Progress

| Milestone | F1 | Features | Notes |
|-----------|-----|----------|-------|
| Baseline (exp1) | 0.6486 | 41 | Original 39+2 features |
| exp10 | 0.6892 | 69 | First counterparty/channel novelty + midnight VA |
| exp36 | 0.6854 | 69 | Top69 pre-screening enabled |
| Target | 0.75 | - | TP_at_150 >= 72 |

## Deployment

```bash
# Remote server (training)
cd /home/ubuntu/autoresearch_lgb
python3 prepare.py

# Local WSL (OpenClaw loop)
cd ~/clawd
openclaw gateway &
# webchat: /prose run /home/ubuntu/clawd/autoresearch_loop.prose

# Monitor
bash ~/monitor_v4.sh
```
