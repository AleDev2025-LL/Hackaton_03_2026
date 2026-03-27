import pandas as pd
import os
import sys

# Asegurar que importamos de config en el nivel superior
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CLASIFICACIONES_FIFO

def run_fifo(df):
    """
    Ejecuta el motor de lotes (FIFO) para las operaciones de compra y venta.
    Retorna un diccionario con resultado_por_activo, stock_final y movimientos_procesados.
    """
    resultados = {}
    stock = {}
    
    if df.empty:
        return {
            "resultado_por_activo": pd.DataFrame(columns=["ticker", "tipo_activo", "cantidad_neta", "costo_usd", "resultado_operacion_ars", "diferencia_cambio_ars", "dividendos_ars", "intereses_ars"]),
            "stock_final": pd.DataFrame(columns=["ticker", "fecha_compra", "cantidad", "precio_compra_usd", "tc_compra", "costo_pesos"]),
            "movimientos_procesados": df
        }
        
    df_procesar = df.sort_values("fecha").copy()
    
    for idx, row in df_procesar.iterrows():
        t = row.get("ticker", "")
        if pd.isna(t) or t == "":
            continue
            
        clas = row.get("clasificacion", "A CLASIFICAR")
        tipo_act = row.get("tipo_activo", "N/A")
        
        # Init structure for ticker
        if t not in resultados:
            resultados[t] = {
                "ticker": t,
                "tipo_activo": tipo_act,
                "cantidad_neta": 0.0,
                "costo_usd": 0.0,
                "resultado_operacion_ars": 0.0,
                "diferencia_cambio_ars": 0.0,
                "dividendos_ars": 0.0,
                "intereses_ars": 0.0
            }
        
        # Mejorar el tipo de activo si estaba en N/A y ahora lo conocemos
        if pd.notna(tipo_act) and tipo_act != "N/A" and resultados[t]["tipo_activo"] == "N/A":
            resultados[t]["tipo_activo"] = tipo_act

        if t not in stock:
            stock[t] = []
            
        if clas in CLASIFICACIONES_FIFO:
            if clas == "COMPRA":
                cant = row["cantidad"]
                if cant > 0:
                    precio = row["precio"] if row["precio"] > 0 else (abs(row["monto_usd"]) / cant if cant > 0 else 0)
                    lote = {
                        "fecha": row["fecha"],
                        "cantidad": cant,
                        "precio_compra": precio,
                        "tc_compra": row.get("tc_comprador", 0.0),
                        "costo_total_pesos": abs(row.get("monto_pesos", 0.0))
                    }
                    stock[t].append(lote)
                    resultados[t]["cantidad_neta"] += cant
                    resultados[t]["costo_usd"] += (precio * cant)
                    
            elif clas in ["VENTA", "REPAGO CAPITAL (VENTA)", "REPAGO CAPITAL (AMORTIZACION)"]:
                cant_vendida = abs(row["cantidad"])
                if cant_vendida == 0:
                    continue
                    
                precio_venta = row["precio"] if row["precio"] > 0 else (abs(row["monto_usd"]) / cant_vendida if cant_vendida > 0 else 0)
                tc_venta = row.get("tc_comprador", 0.0)
                
                # Consumir lotes de stock
                cant_a_cubrir = cant_vendida
                
                while cant_a_cubrir > 0 and len(stock[t]) > 0:
                    lote = stock[t][0]
                    if lote["cantidad"] <= cant_a_cubrir:
                        cant_consumida = lote["cantidad"]
                        stock[t].pop(0)  # Lote completado, lo sacamos
                    else:
                        cant_consumida = cant_a_cubrir
                        stock[t][0]["cantidad"] -= cant_consumida
                    
                    cant_a_cubrir -= cant_consumida
                    
                    # Cálculos exigidos por la regla FIFO
                    costo_asignado_pesos = cant_consumida * lote["precio_compra"] * lote["tc_compra"]
                    ingreso_venta_pesos = cant_consumida * precio_venta * tc_venta
                    res_op = ingreso_venta_pesos - costo_asignado_pesos
                    dif_cambio = cant_consumida * (tc_venta - lote["tc_compra"])
                    
                    resultados[t]["resultado_operacion_ars"] += res_op
                    resultados[t]["diferencia_cambio_ars"] += dif_cambio
                    resultados[t]["costo_usd"] -= (cant_consumida * lote["precio_compra"])
                    
                resultados[t]["cantidad_neta"] -= cant_vendida
                
        elif clas == "DIVIDENDOS":
            resultados[t]["dividendos_ars"] += abs(row.get("monto_pesos", 0.0))
        elif clas == "INTERESES TITULOS":
            resultados[t]["intereses_ars"] += abs(row.get("monto_pesos", 0.0))
            
    df_resultados = pd.DataFrame(list(resultados.values()))
    
    stock_final_list = []
    for t, lotes in stock.items():
        for lote in lotes:
            if lote["cantidad"] > 0:
                stock_final_list.append({
                    "ticker": t,
                    "fecha_compra": lote["fecha"],
                    "cantidad": lote["cantidad"],
                    "precio_compra_usd": lote["precio_compra"],
                    "tc_compra": lote["tc_compra"],
                    "costo_pesos": lote["cantidad"] * lote["precio_compra"] * lote["tc_compra"]
                })
    df_stock_final = pd.DataFrame(stock_final_list)
    
    # Manejar DataFrames vacíos para la UI
    if df_resultados.empty:
        df_resultados = pd.DataFrame(columns=["ticker", "tipo_activo", "cantidad_neta", "costo_usd", "resultado_operacion_ars", "diferencia_cambio_ars", "dividendos_ars", "intereses_ars"])
    if df_stock_final.empty:
        df_stock_final = pd.DataFrame(columns=["ticker", "fecha_compra", "cantidad", "precio_compra_usd", "tc_compra", "costo_pesos"])
        
    return {
        "resultado_por_activo": df_resultados,
        "stock_final": df_stock_final,
        "movimientos_procesados": df_procesar
    }
