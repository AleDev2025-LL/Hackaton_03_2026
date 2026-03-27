import streamlit as st
import pandas as pd
import os
import io
from pdfminer.high_level import extract_text

from modules.classifier import classify_operations
from modules.tc_engine import apply_tc
from modules.fifo_engine import run_fifo
from adapters.schwab_adapter import parse_schwab
from adapters.ubs_adapter import parse_ubs
from adapters.globalwealth_adapter import parse_globalwealth
from config import API_KEY

# Configuración de página
st.set_page_config(page_title="📊 Análisis de Inversiones Exterior — LLA", layout="wide")

# --- FUNCIONES DE SOPORTE ---
def process_pdf_to_text(file):
    """Intenta extraer texto de un PDF cargado."""
    try:
        # pdfminer.high_level.extract_text acepta archivos tipo BytesIO
        text = extract_text(file)
        return text
    except Exception as e:
        # Fallback: intentar lectura como texto plano (por si es un .txt renombrado o similar)
        try:
            file.seek(0)
            return file.read().decode('utf-8')
        except:
            return ""

def save_temp_file(content, suffix=".txt"):
    """Guarda contenido en un archivo temporal para que los adapters existentes lo lean."""
    import tempfile
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, mode='w', encoding='utf-8') as tmp:
        tmp.write(content)
        return tmp.name

# --- SIDEBAR: CARGA Y CONFIGURACIÓN ---
st.sidebar.header("🔑 Configuración")
api_key_input = st.sidebar.text_input("API Key Claude", value=API_KEY, type="password")

st.sidebar.markdown("---")
st.sidebar.header("📂 Cargar Extractos")
file_schwab = st.sidebar.file_uploader("Schwab (CSV)", type=["csv"], key="schwab")
file_ubs = st.sidebar.file_uploader("UBS (PDF/TXT)", type=["pdf", "txt"], key="ubs")
file_gw = st.sidebar.file_uploader("Global Wealth (PDF/TXT)", type=["pdf", "txt"], key="gw")

# Habilitar botón solo si hay algún archivo
can_process = any([file_schwab, file_ubs, file_gw])
btn_procesar = st.sidebar.button("⚡ Procesar", type="primary", disabled=not can_process)

# --- LÓGICA DE PROCESAMIENTO ---
if btn_procesar:
    with st.spinner("Procesando extractos..."):
        all_dfs = []
        fuentes_procesadas = 0
        
        # 1. Cargar Tipo de Cambio de referencia (necesario para el engine)
        try:
            DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
            df_tc = pd.read_csv(os.path.join(DATA_DIR, "tipo_cambio_bna.csv"))
        except:
            st.error("No se encontró el archivo de Tipo de Cambio en /data/tipo_cambio_bna.csv")
            st.stop()

        # 2. Procesar Schwab
        if file_schwab:
            try:
                # Guardar el BytesIO en un archivo temporal para el adapter existente
                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                    tmp.write(file_schwab.getbuffer())
                    tmp_path = tmp.name
                df_s = parse_schwab(tmp_path)
                if not df_s.empty:
                    all_dfs.append(df_s)
                    fuentes_procesadas += 1
                os.unlink(tmp_path)
            except Exception as e:
                st.warning(f"⚠ Schwab: Error al procesar ({e})")

        # 3. Procesar UBS
        if file_ubs:
            try:
                text = ""
                if file_ubs.name.endswith(".pdf"):
                    text = process_pdf_to_text(file_ubs)
                else:
                    text = file_ubs.read().decode('utf-8')
                
                if text:
                    tmp_path = save_temp_file(text)
                    df_u = parse_ubs(tmp_path)
                    if not df_u.empty:
                        all_dfs.append(df_u)
                        fuentes_procesadas += 1
                    os.unlink(tmp_path)
            except Exception as e:
                st.warning(f"⚠ UBS: Error al procesar ({e})")

        # 4. Procesar Global Wealth
        if file_gw:
            try:
                text = ""
                if file_gw.name.endswith(".pdf"):
                    text = process_pdf_to_text(file_gw)
                else:
                    text = file_gw.read().decode('utf-8')
                
                if text:
                    tmp_path = save_temp_file(text)
                    df_g = parse_globalwealth(tmp_path)
                    if not df_g.empty:
                        all_dfs.append(df_g)
                        fuentes_procesadas += 1
                    os.unlink(tmp_path)
            except Exception as e:
                st.warning(f"⚠ Global Wealth: Error al procesar ({e})")

        # 5. Ejecutar Pipeline
        if all_dfs:
            df_unif = pd.concat(all_dfs, ignore_index=True)
            df_clasif = classify_operations(df_unif, api_key_input)
            df_con_tc = apply_tc(df_clasif, df_tc)
            res_fifo = run_fifo(df_con_tc)
            
            # Guardar en Session State
            st.session_state["data_procesada"] = res_fifo
            st.session_state["res_final"] = True
            st.success(f"✅ {len(df_unif)} movimientos procesados de {fuentes_procesadas} fuentes")
        else:
            st.error("No se pudo extraer información válida de los archivos cargados.")

# --- RENDERIZADO DE LA INTERFAZ ---

if "data_procesada" not in st.session_state:
    # Pantalla de Bienvenida
    st.markdown("""
    ## 👋 Bienvenido al Analista de Inversiones Exterior
    Carga tus extractos bancarios en la barra lateral para comenzar el análisis automático.
    
    ### Formatos Soportados:
    - **Schwab**: Archivos CSV exportados directamente de la plataforma.
    - **UBS**: Extractos oficiales en PDF o TXT.
    - **Global Wealth**: Reportes de movimientos en PDF o TXT.
    
    ### ¿Qué hace esta herramienta?
    1. **Extrae** datos de PDFs y CSVs heterogéneos.
    2. **Clasifica** mediante Inteligencia Artificial (Claude-3-5-Sonnet) cada operación.
    3. **Aplica** el Tipo de Cambio histórico del BNA (Dólar Comprador).
    4. **Calcula** Resultados (Gains/Losses) e Impuestos mediante método **FIFO**.
    """)
    
    # Placeholder para logos o estética
    col1, col2, col3 = st.columns(3)
    with col1: st.info("**Charles Schwab**")
    with col2: st.info("**UBS**")
    with col3: st.info("**Global Wealth**")
    
else:
    # Mostrar resultados (Tabs)
    res = st.session_state["data_procesada"]
    df_movs = res["movimientos_procesados"]
    df_res_act = res["resultado_por_activo"]
    df_stock = res["stock_final"]

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
        
        f_fuente = st.multiselect("Filtrar por Fuente", df_movs['fuente'].unique(), default=df_movs['fuente'].unique())
        f_clasif = st.multiselect("Filtrar por Clasificación", df_movs['clasificacion'].unique(), default=df_movs['clasificacion'].unique())
        
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
        saldo_ini_usd = df_g1.iloc[0]["monto_usd"] if not df_g1.empty else 0.0
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
        
        df_g3 = df_res_act.copy()
        if not df_g3.empty:
            df_g3["total_ars"] = df_g3["resultado_operacion_ars"] + df_g3["diferencia_cambio_ars"] + df_g3["dividendos_ars"] + df_g3["intereses_ars"]
            st.dataframe(df_g3, use_container_width=True)
        else:
            st.info("No hay transacciones de compra/venta para calcular resultados FIFO.")

    with tab4:
        st.subheader("Stock Final FIFO (G-4)")
        col1, col2 = st.columns(2)
        col1.metric("Activos con Posición Abierta", len(df_stock["ticker"].unique()) if not df_stock.empty else 0)
        col2.metric("Valor Estimado Costo Histórico (ARS)", f"${df_stock['costo_pesos'].sum():,.2f}" if not df_stock.empty else "$0.00")
        
        if not df_stock.empty:
            st.dataframe(df_stock, use_container_width=True)
        else:
            st.warning("No quedó stock abierto al final del periodo.")
