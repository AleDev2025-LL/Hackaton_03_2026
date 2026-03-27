import pandas as pd
import numpy as np

def apply_tc(df, tc_df):
    """
    Aplica el tipo de cambio a las operaciones.
    Usa el tc_comprador de la fecha exacta o el último anterior disponible.
    """
    if df.empty or tc_df.empty:
        df_copy = df.copy()
        df_copy["tc_comprador"] = 0.0
        df_copy["monto_pesos"] = 0.0
        return df_copy
        
    df = df.copy()
    tc_df = tc_df.copy()
    
    df["fecha_orig"] = df["fecha"]
    df["fecha"] = pd.to_datetime(df["fecha"]).dt.normalize()
    tc_df["fecha"] = pd.to_datetime(tc_df["fecha"]).dt.normalize()
    
    # Ordenar por fecha para merge_asof
    df = df.sort_values(by="fecha")
    tc_df = tc_df.sort_values(by="fecha")
    
    # Hacer merge asof backward (toma el mismo día o anterior)
    merged = pd.merge_asof(
        df, 
        tc_df[["fecha", "tc_comprador"]], 
        on="fecha", 
        direction="backward"
    )
    
    # En caso de fechas anteriores al csv, propagar backward (por si acaso quedaron nulls al principio)
    merged["tc_comprador"] = merged["tc_comprador"].bfill().fillna(1.0) # Fallback 1.0 por si falla todo
    
    merged["monto_pesos"] = merged.apply(lambda row: float(row["monto_usd"]) * float(row["tc_comprador"]) if float(row["monto_usd"]) != 0 else 0.0, axis=1)
    
    # Restaurar orden cronológica general y fecha original si corresponde
    merged["fecha"] = merged["fecha_orig"]
    merged.drop(columns=["fecha_orig"], inplace=True)
    
    return merged
