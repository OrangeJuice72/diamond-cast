
# DiamondCast Projector

A starter app that combines:
- Python backend (Flask)
- spreadsheet-driven model config (`model_config.xlsx`)
- app-style HTML frontend

## Features
- Pick a date
- Load daily MLB games
- Choose a game
- Project score for that matchup
- Show weather at the ballpark
- Show projected pitcher lines
- Show projected hitter lines
- Display local logo badges
- Render a simple overhead ballpark/weather graphic

## Data sources used by the app
- MLB Stats API for schedules, rosters, team stats, player stats, and probable pitchers
- Open-Meteo for hourly weather / historical weather

## Run locally
```bash
cd mlb_weather_projection_app
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Then open: http://127.0.0.1:8000

## Spreadsheet
Edit `model_config.xlsx` to tune:
- ballpark metadata
- altitude
- park factors
- run model weights

## Notes
This is a practical starter model, not a betting-grade simulator.
The projection logic is intentionally transparent and easy to tweak.
