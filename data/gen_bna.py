import pandas as pd
import numpy as np

fechas = pd.date_range(start="2024-01-01", end="2025-12-31", freq="B")
df = pd.DataFrame({"fecha": fechas})
n_days = len(df)
np.random.seed(42)
tc_comprador = np.linspace(800.0, 1050.0, n_days)
noise = np.random.normal(0, 1.5, n_days)
df["tc_comprador"] = np.round(tc_comprador + noise, 2)
df["tc_vendedor"] = np.round(df["tc_comprador"] * 1.05, 2) 

df.to_csv("C:/Users/aneustadt/Codigo/Hackaton/MVPs/caso05_inversiones/data/tipo_cambio_bna.csv", index=False)
