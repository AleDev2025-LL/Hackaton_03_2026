import pandas as pd
import numpy as np

def parse_schwab(filepath):
    df = pd.read_csv(filepath)
    result = []
    
    for _, row in df.iterrows():
        try:
            fecha = pd.to_datetime(row["Date"]) if pd.notna(row["Date"]) else None
        except:
            fecha = None
        
        amt = str(row["Amount"]).replace(",", "") if pd.notna(row["Amount"]) else "0"
        try:
            monto_usd = float(amt)
        except:
            monto_usd = 0.0
            
        qty = str(row["Quantity"]).replace(",", "") if pd.notna(row["Quantity"]) else "0"
        try:
            cantidad = float(qty)
        except:
            cantidad = 0.0
            
        prc = str(row["Price"]).replace(",", "") if pd.notna(row["Price"]) else "0"
        try:
            precio = float(prc)
        except:
            precio = 0.0
            
        result.append({
            "fecha": fecha,
            "fuente": "SCHWAB",
            "tipo_raw": str(row["Action"]) if pd.notna(row["Action"]) else "",
            "ticker": str(row["Symbol"]) if pd.notna(row["Symbol"]) else "",
            "detalle": str(row["Description"]) if pd.notna(row["Description"]) else "",
            "monto_usd": monto_usd,
            "cantidad": cantidad,
            "precio": precio
        })
        
    return pd.DataFrame(result)
