import pandas as pd
import json
import re
import math
from anthropic import Anthropic
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MODELO, BATCH_SIZE, CLASIFICACIONES_VALIDAS, TIPOS_ACTIVO_VALIDOS

def extract_json(text):
    """
    Intenta extraer y parsear un JSON array de la respuesta del LLM.
    Si Claude incluye markdown o texto adicional, esto lo filtra.
    """
    match = re.search(r'\[.*\]', text, re.DOTALL)
    if match:
        return json.loads(match.group(0))
    # Si no tiene corchetes, asumimos que puede venir limpio
    return json.loads(text)

def classify_operations(df, api_key):
    """
    Clasifica las operaciones usando Claude en batches.
    Asigna 'clasificacion', 'tipo_activo' y 'metodo_clasificacion'.
    """
    df_result = df.copy()
    df_result["clasificacion"] = "A CLASIFICAR"
    df_result["tipo_activo"] = "OTROS"
    df_result["metodo_clasificacion"] = "ERROR"
    
    if df_result.empty or not api_key:
        return df_result
        
    client = Anthropic(api_key=api_key)
    system_prompt = "Eres un clasificador de operaciones financieras de bancos del exterior argentinos. Dado un movimiento financiero, devuelve SOLO un JSON con dos campos: clasificacion y tipo_activo. No agregues texto adicional."
    
    num_batches = math.ceil(len(df_result) / BATCH_SIZE)
    
    for i in range(num_batches):
        batch_idx = list(range(i * BATCH_SIZE, min((i + 1) * BATCH_SIZE, len(df_result))))
        batch_df = df_result.iloc[batch_idx]
        
        user_prompt = f"Clasificá estas {len(batch_idx)} operaciones financieras:\n"
        for local_idx, (global_idx, row) in enumerate(batch_df.iterrows()):
            user_prompt += f"{local_idx + 1}. tipo_raw: {row.get('tipo_raw', '')} | detalle: {row.get('detalle', '')}\n"
            
        user_prompt += f"...\nDevolvé SOLO un JSON array con {len(batch_idx)} objetos: [{{clasificacion, tipo_activo}}, ...]"
        
        try:
            response = client.messages.create(
                model=MODELO,
                max_tokens=2048,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": user_prompt}
                ]
            )
            
            response_text = response.content[0].text
            try:
                parsed_json = extract_json(response_text)
                
                # Validar y asignar
                if len(parsed_json) == len(batch_idx):
                    for local_idx, global_idx in enumerate(batch_idx):
                        c = parsed_json[local_idx].get("clasificacion", "A CLASIFICAR").upper()
                        t = parsed_json[local_idx].get("tipo_activo", "OTROS").upper()
                        
                        # Validar contra constantes
                        if c not in CLASIFICACIONES_VALIDAS:
                            c = "A CLASIFICAR"
                        if t not in TIPOS_ACTIVO_VALIDOS:
                            t = "OTROS"
                            
                        df_result.iloc[global_idx, df_result.columns.get_loc("clasificacion")] = c
                        df_result.iloc[global_idx, df_result.columns.get_loc("tipo_activo")] = t
                        df_result.iloc[global_idx, df_result.columns.get_loc("metodo_clasificacion")] = "LLM"
            except (json.JSONDecodeError, AttributeError):
                pass  # Si falla, se queda en ERROR / A CLASIFICAR
        except Exception:
            pass  # Error de API, se queda en ERROR
            
    return df_result
