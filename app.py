import streamlit as st
import pandas as pd
import os

from modules.classifier import classify_operations
from modules.tc_engine import apply_tc
from modules.fifo_engine import run_fifo
from adapters.schwab_adapter import parse_schwab
from adapters.ubs_adapter import parse_ubs
from adapters.globalwealth_adapter import parse_globalwealth
from config import API_KEY

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

st.set_page_config(page_title="📊 Análisis de Inversiones Exterior — LLA", layout="wide")
st.title("📊 Análisis de Inversiones Exterior — LLA")

st.sidebar.header("Configuración")
api_key = st.sidebar.text_input("API Key Claude", value=API_KEY, type="password")

if st.sidebar.button("Procesar"):
    with st.spinner("Procesando..."):
        try:
            df_schw = parse_schwab(os.path.join(DATA_DIR, "schwab_sample.csv"))
            df_ubs = parse_ubs(os.path.join(DATA_DIR, "ubs_sample.txt"))
            df_gw = parse_globalwealth(os.path.join(DATA_DIR, "globalwealth_sample.txt"))
            df_tc = pd.read_csv(os.path.join(DATA_DIR, "tipo_cambio_bna.csv"))
        except Exception as e:
            st.error(f"Error cargando archivos de muestra: {e}")
            st.stop()
            
        df_unif = pd.concat([df_schw, df_ubs, df_gw], ignore_index=True)
        # Classify
        df_clasif = classify_operations(df_unif, api_key)
        # Apply TC
        df_con_tc = apply_tc(df_clasif, df_tc)
        # Run FIFO
        res_fifo = run_fifo(df_con_tc)
        
        df_movs = res_fifo["movimientos_procesados"]
        df_res_act = res_fifo["resultado_por_activo"]
        df_stock = res_fifo["stock_final"]

        tab1, tab2, tab3, tab4 = st.tabs([
            "📋 Movimientos Clasificados", 
            "💱 Detalle con TC (G-1)", 
            "📈 Resultado por Activo (G-3)", 
            "📦 Stock Final FIFO (G-4)"
        ])
        
        with tab1:
            st.subheader("Movimientos Clasificados")
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Movimientos", len(df_movs))
            col2.metric("Total A CLASIFICAR", len(df_movs[df_movs['clasificacion'] == 'A CLASIFICAR']))
            col3.metric("Total Monto USD", f"${df_movs['monto_usd'].sum():,.2f}")
            
            f_fuente = st.multiselect("Fuente", df_movs['fuente'].unique(), default=df_movs['fuente'].unique())
            f_clasif = st.multiselect("Clasificación", df_movs['clasificacion'].unique(), default=df_movs['clasificacion'].unique())
            
            if not f_fuente: f_fuente = df_movs['fuente'].unique()
            if not f_clasif: f_clasif = df_movs['clasificacion'].unique()
            
            df_filt = df_movs[(df_movs['fuente'].isin(f_fuente)) & (df_movs['clasificacion'].isin(f_clasif))].copy()
            df_filt['detalle'] = df_filt['detalle'].astype(str).apply(lambda x: x[:50] + "..." if len(x) > 50 else x)
            
            cols_to_show = ["fecha", "fuente", "ticker", "detalle", "clasificacion", "tipo_activo", "monto_usd", "tc_comprador", "monto_pesos"]
            
            def highlight_aclasificar(s):
                return ['background-color: #ffd700; color: black' if s['clasificacion'] == 'A CLASIFICAR' else '' for _ in s]
                
            st.dataframe(df_filt[cols_to_show].style.apply(highlight_aclasificar, axis=1), use_container_width=True)
            
        with tab2:
            st.subheader("Detalle con TC (G-1)")
            df_g1 = df_movs.sort_values("fecha").copy()
            df_g1["saldo_acumulado_pesos"] = df_g1["monto_pesos"].cumsum()
            
            col1, col2, col3 = st.columns(3)
            
            if "SALDO INICIAL" in df_g1["clasificacion"].values:
                saldo_ini_usd = df_g1.loc[df_g1["clasificacion"] == "SALDO INICIAL", "monto_usd"].sum()
            elif not df_g1.empty:
                saldo_ini_usd = df_g1.iloc[0]["monto_usd"]
            else:
                saldo_ini_usd = 0.0
                
            saldo_fin_usd = df_g1["monto_usd"].sum()
            tc_promedio = df_g1[df_g1["monto_usd"] != 0]["tc_comprador"].mean()
            
            col1.metric("Saldo Inicial USD", f"${saldo_ini_usd:,.2f}")
            col2.metric("Saldo Final USD", f"${saldo_fin_usd:,.2f}")
            col3.metric("TC Promedio Aplicado", f"${tc_promedio:,.2f}" if not pd.isna(tc_promedio) else "-")
            
            st.dataframe(df_g1[["fecha", "fuente", "detalle", "monto_usd", "tc_comprador", "monto_pesos", "saldo_acumulado_pesos"]], use_container_width=True)
            
        with tab3:
            st.subheader("Resultado por Activo (G-3)")
            col1, col2 = st.columns(2)
            col1.metric("Resultado Operativo Total (ARS)", f"${df_res_act['resultado_operacion_ars'].sum():,.2f}")
            col2.metric("Diferencia de Cambio Total (ARS)", f"${df_res_act['diferencia_cambio_ars'].sum():,.2f}")
            
            if not df_res_act.empty:
                df_g3 = df_res_act.copy()
                df_g3["total_ars"] = df_g3["resultado_operacion_ars"] + df_g3["diferencia_cambio_ars"] + df_g3["dividendos_ars"] + df_g3["intereses_ars"]
                
                total_row = pd.DataFrame([{
                    "ticker": "TOTAL",
                    "tipo_activo": "",
                    "cantidad_neta": df_g3["cantidad_neta"].sum(),
                    "costo_usd": df_g3["costo_usd"].sum(),
                    "resultado_operacion_ars": df_g3["resultado_operacion_ars"].sum(),
                    "diferencia_cambio_ars": df_g3["diferencia_cambio_ars"].sum(),
                    "dividendos_ars": df_g3["dividendos_ars"].sum(),
                    "intereses_ars": df_g3["intereses_ars"].sum(),
                    "total_ars": df_g3["total_ars"].sum()
                }])
                df_g3_disp = pd.concat([df_g3, total_row], ignore_index=True)
                
                st.dataframe(df_g3_disp, use_container_width=True)
            else:
                st.info("No hay resultados de operaciones (FIFO) disponibles.")
                
        with tab4:
            st.subheader("Stock Final FIFO (G-4)")
            col1, col2 = st.columns(2)
            col1.metric("Activos con Posición Abierta", len(df_stock["ticker"].unique()) if not df_stock.empty else 0)
            col2.metric("Valor Estimado Costo Histórico (ARS)", f"${df_stock['costo_pesos'].sum():,.2f}" if not df_stock.empty else "$0.00")
            
            if not df_stock.empty:
                st.dataframe(df_stock, use_container_width=True)
            else:
                st.info("No quedó stock abierto.")
