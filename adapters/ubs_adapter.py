import pandas as pd
import re

def parse_ubs(filepath):
    result = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip(): continue
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 4:
                try:
                    fecha = pd.to_datetime(parts[0])
                except Exception:
                    continue
                    
                tipo_raw = parts[1]
                detalle = parts[2]
                
                amt_str = parts[3].replace(",", "")
                try:
                    monto_usd = float(amt_str)
                except ValueError:
                    monto_usd = 0.0
                    
                # Extraer ticker rudimentario a fines del MVP si está en la convención UBS
                ticker = ""
                if "SYMBOL:" in detalle:
                    match = re.search(r"SYMBOL:\s*([A-Za-z]+)", detalle)
                    if match: ticker = match.group(1)
                elif "SHARES" in detalle:
                    match = re.search(r"SHARES\s+([A-Za-z]+)", detalle)
                    if match: ticker = match.group(1)
                    
                # Como el TXT no tiene cant/precio, inferimos si 'BOUGHT' / 'SOLD' está en detalle
                cantidad = 0.0
                precio = 0.0
                
                # Intentar parsear "BOUGHT 100 SHARES"
                qty_match = re.search(r"(?:BOUGHT|SOLD)\s+([\d\.]+)\s+SHARES", detalle)
                if qty_match:
                    try:
                        cantidad = float(qty_match.group(1))
                        # Si es venta ponemos negativa
                        if "SOLD" in detalle:
                            cantidad = -cantidad
                    except ValueError:
                        pass
                
                result.append({
                    "fecha": fecha,
                    "fuente": "UBS",
                    "tipo_raw": tipo_raw,
                    "ticker": ticker,
                    "detalle": detalle,
                    "monto_usd": monto_usd,
                    "cantidad": cantidad,
                    "precio": precio
                })
                
    return pd.DataFrame(result)
