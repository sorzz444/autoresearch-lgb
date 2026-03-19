#!/bin/bash
# OpenClaw Autoresearch Pipeline Monitor v4
# Real-time pipeline progress monitor (including prepare.py internal steps)

REMOTE="${REMOTE_HOST:-ubuntu@YOUR_SERVER_IP}"
RESULTS_FILE="/home/ubuntu/autoresearch_lgb/results.tsv"
TRAIN_FILE="/home/ubuntu/autoresearch_lgb/train.py"
PROGRESS_FILE="/home/ubuntu/autoresearch_lgb/artifacts/progress.txt"
LAST_FEATURES="/home/ubuntu/autoresearch_lgb/last_features.txt"
IV_FILE="/home/ubuntu/autoresearch_lgb/artifacts/iv_psi_results.xlsx"
LOCAL_REFS="/home/ubuntu/clawd/autoresearch_data/references/"
SESSION_DIR="/home/ubuntu/.openclaw/agents/main/sessions"
TARGET_F1="0.75"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
GRAY='\033[0;90m'
NC='\033[0m' # No Color
BOLD='\033[1m'
BG_YELLOW='\033[43m'

clear_screen() {
    printf '\033[2J\033[H'
}

while true; do
    clear_screen
    NOW=$(date '+%Y-%m-%d %H:%M:%S')
    
    # === Header ===
    echo -e "${WHITE}══════════════════════════════════════════════════════════${NC}"
    echo -e "   OpenClaw Autoresearch Pipeline Monitor v4"
    echo -e "${WHITE}══════════════════════════════════════════════════════════${NC}"
    echo -e "Time: $NOW"
    
    # === Gateway Status ===
    GW_PID=$(pgrep -f openclaw-gateway 2>/dev/null | head -1)
    if [ -n "$GW_PID" ]; then
        echo -e " ${GREEN}●${NC} Gateway: Running (PID $GW_PID)"
    else
        echo -e " ${RED}●${NC} Gateway: ${RED}Stopped${NC}"
    fi
    
    # === Best F1 ===
    BEST_F1=$(ssh -o ConnectTimeout=3 $REMOTE "awk -F'\t' 'NR>1 && \$3+0>0 {if(\$3+0>max) max=\$3+0} END{print max}' $RESULTS_FILE 2>/dev/null" 2>/dev/null)
    [ -z "$BEST_F1" ] && BEST_F1="N/A"
    echo -e " 🏆 Best F1: ${GREEN}${BEST_F1}${NC}  Target: ${TARGET_F1}"
    
    # === Pipeline Status ===
    echo -e "──────────── ${BOLD}Pipeline Status${NC} ────────────"
    
    # Get remote progress
    REMOTE_PROGRESS=$(ssh -o ConnectTimeout=3 $REMOTE "cat $PROGRESS_FILE 2>/dev/null" 2>/dev/null)
    
    # Check if prepare.py is running
    PREPARE_RUNNING=$(ssh -o ConnectTimeout=3 $REMOTE "ps aux | grep 'prepare.py' | grep -v grep | head -1" 2>/dev/null)
    
    # Check if train.py was recently modified (Todd editing)
    TRAIN_MTIME=$(ssh -o ConnectTimeout=3 $REMOTE "stat -c %Y $TRAIN_FILE 2>/dev/null" 2>/dev/null)
    CURRENT_TS=$(date +%s)
    TRAIN_AGE=$((CURRENT_TS - ${TRAIN_MTIME:-0}))
    
    # Check session activity
    LATEST_SESSION=$(ls -t $SESSION_DIR/*.jsonl 2>/dev/null | head -1)
    SESSION_SIZE=""
    SESSION_TIME=""
    if [ -n "$LATEST_SESSION" ]; then
        SESSION_SIZE=$(stat -c %s "$LATEST_SESSION" 2>/dev/null)
        SESSION_TIME=$(stat -c %Y "$LATEST_SESSION" 2>/dev/null)
        SESSION_AGE=$((CURRENT_TS - ${SESSION_TIME:-0}))
    fi
    
    # Determine current phase
    CURRENT_STEP=""
    if [ -n "$PREPARE_RUNNING" ]; then
        # prepare.py is running - use progress file
        if [ -n "$REMOTE_PROGRESS" ]; then
            CURRENT_STEP="$REMOTE_PROGRESS"
        else
            CURRENT_STEP="4.Feature Engineering"  # default if no progress yet
        fi
    elif [ -n "$SESSION_SIZE" ] && [ "$SESSION_AGE" -lt 60 ]; then
        # Session recently active, Todd is working
        if [ "$TRAIN_AGE" -lt 120 ]; then
            CURRENT_STEP="3.Modify train.py"
        elif [ "$SESSION_AGE" -lt 30 ]; then
            # Check if reading references
            RECENT_READ=$(grep "references\|missed_account\|missed_analysis" "$LATEST_SESSION" 2>/dev/null | tail -1)
            if echo "$RECENT_READ" | grep -q "references" 2>/dev/null; then
                CURRENT_STEP="0.Read References"
            elif echo "$RECENT_READ" | grep -q "missed_account" 2>/dev/null; then
                CURRENT_STEP="2.Analyze Flows+Design Features"
            else
                CURRENT_STEP="1.Check Status/Download Flows"
            fi
        fi
    fi
    
    # All pipeline steps
    STEPS=(
        "0.Read References"
        "1.Check Status/Download Flows"
        "2.Analyze Flows+Design Features"
        "3.Modify train.py"
        "4.Feature Engineering"
        "5.IV/PSI Filtering"
        "6.Correlation Filter(>=0.95)"
        "7.PreScreen LGB(Top69)"
        "8.Optuna HPO(100trials)"
        "9.Remove importance=0"
        "10.OOF 5-fold CV"
        "11.Top-K F1 Eval"
        "12.Pull Missed Flows"
        "13.Write Results/Git"
    )
    
    for step in "${STEPS[@]}"; do
        STEP_NUM=$(echo "$step" | grep -oP '^\d+')
        
        if [ -n "$CURRENT_STEP" ]; then
            CURRENT_NUM=$(echo "$CURRENT_STEP" | grep -oP '^\d+')
            
            if [ "$STEP_NUM" = "$CURRENT_NUM" ]; then
                # Current step - highlighted
                echo -e "  ${BG_YELLOW}${BOLD} ▶ ${step} ${NC}  ← Current"
            elif [ "$STEP_NUM" -lt "$CURRENT_NUM" ] 2>/dev/null; then
                # Completed step
                echo -e "  ${GREEN}✓${NC} ${step}"
            else
                # Future step
                echo -e "  ${GRAY}○ ${step}${NC}"
            fi
        else
            # No current step - all gray (idle)
            echo -e "  ${GRAY}○ ${step}${NC}"
        fi
    done
    
    # Current status summary
    if [ -n "$CURRENT_STEP" ]; then
        if [ -n "$PREPARE_RUNNING" ]; then
            # Show prepare.py elapsed time
            PREP_START=$(echo "$PREPARE_RUNNING" | awk '{print $9}' 2>/dev/null)
            echo -e " Current: ${CYAN}${CURRENT_STEP}${NC} (prepare.pyRunning ${PREP_START})"
        else
            echo -e " Current: ${CYAN}Agent thinking — ${CURRENT_STEP}${NC}"
        fi
    else
        echo -e " Current: ${GRAY}Idle${NC}"
    fi
    
    # === Experiment Records ===
    echo -e "──────────── ${BOLD}Experiment Log${NC} ────────────"
    RESULTS=$(ssh -o ConnectTimeout=3 $REMOTE "tail -6 $RESULTS_FILE 2>/dev/null" 2>/dev/null)
    if [ -n "$RESULTS" ]; then
        echo "$RESULTS" | while IFS= read -r line; do
            if echo "$line" | grep -q "GATE_FAIL\|GATE_WARN"; then
                echo -e " ${RED}${line}${NC}"
            elif echo "$line" | grep -q "timestamp"; then
                continue  # skip header
            else
                # Check if this is the best result
                F1=$(echo "$line" | awk -F'\t' '{print $3}')
                if [ "$F1" = "$BEST_F1" ] 2>/dev/null; then
                    echo -e " ${GREEN}${BOLD}${line}${NC}  ← 🏆"
                else
                    echo -e " ${line}"
                fi
            fi
        done
    fi
    
    # === New Features ===
    echo -e "──────────── ${BOLD}Feature Changes${NC} ────────────"
    
    # Compare current train.py FEATURES with last commit
    NEW_FEATS=$(ssh -o ConnectTimeout=3 $REMOTE "cd /home/ubuntu/autoresearch_lgb && git diff HEAD -- train.py 2>/dev/null | grep '^+.*_r[0-9]' | grep -v '#+' | sed \"s/^+[ ]*'//;s/',.*//;s/^+[ ]*//\" | head -20" 2>/dev/null)
    
    if [ -n "$NEW_FEATS" ]; then
        echo -e " ${CYAN}New features (uncommitted):${NC}"
        
        # Try to get IV values
        IV_DATA=$(ssh -o ConnectTimeout=3 $REMOTE "python3 -c \"
import pandas as pd
try:
    df = pd.read_excel('$IV_FILE' if '$IV_FILE' else '', engine='openpyxl')
    for _, r in df.iterrows():
        print(f'{r[\"feature\"]}|{r[\"iv\"]:.4f}')
except:
    pass
\" 2>/dev/null" 2>/dev/null)
        
        echo "$NEW_FEATS" | while IFS= read -r feat; do
            feat_clean=$(echo "$feat" | tr -d "' ,+" | xargs)
            [ -z "$feat_clean" ] && continue
            
            # Look up IV
            IV_VAL=""
            if [ -n "$IV_DATA" ]; then
                IV_VAL=$(echo "$IV_DATA" | grep "^${feat_clean}|" | cut -d'|' -f2)
            fi
            
            if [ -n "$IV_VAL" ]; then
                IV_NUM=$(echo "$IV_VAL" | awk '{printf "%.4f", $1}')
                IV_CHECK=$(echo "$IV_NUM" | awk '{print ($1 >= 0.1) ? "high" : ($1 >= 0.02) ? "ok" : "low"}')
                if [ "$IV_CHECK" = "high" ]; then
                    echo -e "   ${GREEN}★ ${feat_clean}  IV=${IV_NUM}${NC}"
                elif [ "$IV_CHECK" = "ok" ]; then
                    echo -e "   ${WHITE}  ${feat_clean}  IV=${IV_NUM}${NC}"
                else
                    echo -e "   ${RED}✗ ${feat_clean}  IV=${IV_NUM} (will be filtered)${NC}"
                fi
            else
                echo -e "   ${GRAY}  ${feat_clean}  (IV pending)${NC}"
            fi
        done
    else
        # No new features - show current feature count
        FEAT_COUNT=$(ssh -o ConnectTimeout=3 $REMOTE "grep -c '_r[0-9]' /home/ubuntu/autoresearch_lgb/train.py 2>/dev/null" 2>/dev/null)
        echo -e " ${GRAY}No new features | Current FEATURES list ~ ${FEAT_COUNT:-?} custom features${NC}"
    fi
    
    # === Recent Logs ===
    echo -e "──────────── ${BOLD}Recent Logs${NC} ────────────"
    grep -v "whatsapp\|WhatsApp\|channel exited" ~/openclaw.log 2>/dev/null | tail -3 | while IFS= read -r line; do
        if echo "$line" | grep -qi "error\|fail\|timeout"; then
            echo -e " ${RED}${line}${NC}"
        else
            echo -e " ${GRAY}${line}${NC}"
        fi
    done
    
    echo -e "---------- 15s refresh ----------"
    sleep 15
done
