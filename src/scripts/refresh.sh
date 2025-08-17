#!/bin/bash

REQUEST_ID=${AWS_REQUEST_ID:-"UNKNOWN_REQUEST"} # Added for better Lambda logging

echo "[${REQUEST_ID}] refresh.sh started. Executing Python scripts in succession..."

# update player_gamelogs
echo "[${REQUEST_ID}] Running update_gamelogs.py..."
python3 src/scripts/update_gamelogs.py
if [ $? -ne 0 ]; then
    echo "[${REQUEST_ID}] ERROR: update_gamelogs.py failed. Exiting."
    exit 1
fi
echo "[${REQUEST_ID}] update_gamelogs.py finished."

# update team_gamelogs
echo "[${REQUEST_ID}] Running get_team_gamelogs.py..."
python3 src/scripts/get_team_gamelogs.py
if [ $? -ne 0 ]; then
    echo "[${REQUEST_ID}] ERROR: get_team_gamelogs.py failed. Exiting."
    exit 1
fi
echo "[${REQUEST_ID}] get_team_gamelogs.py finished."

# update play_by_play
echo "[${REQUEST_ID}] Running new_plays.py..."
python3 src/scripts/new_plays.py
if [ $? -ne 0 ]; then
    echo "[${REQUEST_ID}] ERROR: new_plays.py failed. Exiting."
    exit 1
fi
echo "[${REQUEST_ID}] new_plays.py finished."

# update misc metrics
echo "[${REQUEST_ID}] Running get_misc_metrics_logs.py..."
python3 src/scripts/get_misc_metrics_logs.py
if [ $? -ne 0 ]; then
    echo "[${REQUEST_ID}] ERROR: get_misc_metrics_logs.py failed. Exiting."
    exit 1
fi
echo "[${REQUEST_ID}] get_misc_metrics_logs.py finished."

# update player advanced metrics
echo "[${REQUEST_ID}] Running get_player_advanced_metrics.py..."
python3 src/scripts/get_player_advanced_metrics.py
if [ $? -ne 0 ]; then
    echo "[${REQUEST_ID}] ERROR: get_player_advanced_metrics.py failed. Exiting."
    exit 1
fi
echo "[${REQUEST_ID}] get_player_advanced_metrics.py finished."

# update team advanced metrics
echo "[${REQUEST_ID}] Running get_team_advanced_metrics.py..."
python3 src/scripts/get_playerget_team_advanced_metrics_advanced_metrics.py
if [ $? -ne 0 ]; then
    echo "[${REQUEST_ID}] ERROR: get_team_advanced_metrics.py failed. Exiting."
    exit 1
fi
echo "[${REQUEST_ID}] get_team_advanced_metrics.py finished."

echo "[${REQUEST_ID}] All Python scripts in refresh.sh completed successfully."
exit 0 # Exit with success status