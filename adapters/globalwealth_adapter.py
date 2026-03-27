import pandas as pd
import re

def parse_globalwealth(filepath):
    result = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip(): continue
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 4:
                tipo_raw = parts[0]
                detalle = parts[1]
                
                try:
                    fecha = pd.to_datetime(parts[2])
                except Exception:
                    continue
                    
                amt_str = parts[3].replace(",", "")
                try:
                    monto_usd = float(amt_str)
                except ValueError:
                    monto_usd = 0.0
                
                # Extracción rudimentaria de ticker
                ticker = ""
                if " APPLE INC" in detalle.upper():
                    ticker = "AAPL"
                    
                result.append({
                    "fecha": fecha,
                    "fuente": "GLOBAL WEALTH",
                    "tipo_raw": tipo_raw,
                    "ticker": ticker,
                    "detalle": detalle,
                    "monto_usd": monto_usd,
                    "cantidad": 0.0,
                    "precio": 0.0
                })
                
    return pd.DataFrame(result)
