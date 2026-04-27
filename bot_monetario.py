import requests
import pandas as pd
import json
import warnings
import os
import time

warnings.filterwarnings('ignore')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

VARS_MENSUAL = {
    110: 'adelantos', 111: 'documentos', 112: 'hipotecarios', 113: 'prendarios', 114: 'personales', 115: 'tarjetas',
    85: 'cc', 86: 'ca', 24: 'plazo_fijo', 108: 'depositos_usd', 125: 'prestamos_usd',
    1: 'reservas', 78: 'compra_divisas', 5: 'tc_mayorista', 4: 'tc_minorista',
    27: 'ipc_mensual', 28: 'ipc_interanual', 29: 'rem_interanual',
    15: 'base_monetaria', 109: 'm2', 197: 'm2_transaccional', 16: 'circulacion_monetaria', 17: 'billetes_monedas',
    152: 'pases_pasivos', 196: 'lefi',
    161: 'tasa_politica_tea', 35: 'badlar_tea', 45: 'tamar_tea', 11: 'baibar_tea', 8: 'tm20_tea'
}
VARS_DIARIO = {35: 'badlar_tea', 45: 'tamar_tea'}

# Disfraz para engañar a los firewalls antibots
HEADERS_BOT = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'es-AR,es;q=0.9,en-US;q=0.8,en;q=0.7'
}

def fetch_itcrm_excel():
    print("Descargando ITCRM y Bilaterales (Scrapeando Excel BCRA)...")
    temp_path = os.path.join(BASE_DIR, 'itcrm_temp.xlsx')
    try:
        r = requests.get("https://www.bcra.gob.ar/archivos/Pdfs/PublicacionesEstadisticas/ITCRMSerie.xlsx", headers=HEADERS_BOT, verify=False, timeout=30)
        if r.status_code == 200:
            with open(temp_path, 'wb') as f: f.write(r.content)
            df = pd.read_excel(temp_path, sheet_name=0, skiprows=1)
            col_map = {}
            for c in df.columns:
                c_str = str(c).strip().lower()
                if 'fecha' in c_str or 'período' in c_str or 'periodo' in c_str: col_map[c] = 'fecha'
                elif c_str == 'itcrm': col_map[c] = 'itcrm'
                elif 'brasil' in c_str: col_map[c] = 'tcr_brasil'
                elif 'canadá' in c_str or 'canada' in c_str: col_map[c] = 'tcr_canada'
                elif 'chile' in c_str: col_map[c] = 'tcr_chile'
                elif 'estados unidos' in c_str or 'usa' in c_str: col_map[c] = 'tcr_usa'
                elif 'méxico' in c_str or 'mexico' in c_str: col_map[c] = 'tcr_mexico'
                elif 'uruguay' in c_str: col_map[c] = 'tcr_uruguay'
                elif 'china' in c_str: col_map[c] = 'tcr_china'
                elif 'india' in c_str: col_map[c] = 'tcr_india'
                elif 'japón' in c_str or 'japon' in c_str: col_map[c] = 'tcr_japon'
                elif 'reino unido' in c_str: col_map[c] = 'tcr_reino_unido'
                elif 'suiza' in c_str: col_map[c] = 'tcr_suiza'
                elif 'zona euro' in c_str: col_map[c] = 'tcr_zona_euro'
                elif 'vietnam' in c_str: col_map[c] = 'tcr_vietnam'
            df = df.rename(columns=col_map)
            cols_to_keep = list(col_map.values())
            df = df[cols_to_keep].dropna(subset=['fecha'])
            df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
            for c in cols_to_keep:
                if c != 'fecha': df[c] = pd.to_numeric(df[c], errors='coerce')
            if os.path.exists(temp_path): os.remove(temp_path)
            return df
    except Exception as e: print(f"  -> Error Excel ITCRM: {e}")
    return pd.DataFrame(columns=['fecha'])

def fetch_dolares_history():
    print("Descargando Dólares Históricos (MEP y Blue)...")
    df_d = pd.DataFrame(columns=['fecha'])
    
    # MEP
    try:
        r = requests.get("https://api.argentinadatos.com/v1/cotizaciones/dolares/bolsa", headers=HEADERS_BOT, timeout=20, verify=False)
        if r.status_code == 200:
            df_mep = pd.DataFrame(r.json())
            df_mep['fecha'] = pd.to_datetime(df_mep['fecha'])
            df_d = pd.merge(df_d, df_mep[['fecha', 'venta']].rename(columns={'venta': 'dolar_mep'}), on='fecha', how='outer')
    except:
        print("  -> ArgentinaDatos bloqueado para MEP. Usando Ámbito...")
        try:
            now = pd.Timestamp.now().strftime('%Y-%m-%d')
            r = requests.get(f"https://mercados.ambito.com//dolar/mep/historico-general/2004-01-01/{now}", headers=HEADERS_BOT, verify=False)
            data = r.json()
            if len(data) > 1:
                df_mep = pd.DataFrame(data[1:], columns=data[0])
                df_mep['fecha'] = pd.to_datetime(df_mep['Fecha'], format='%d-%m-%Y', errors='coerce')
                df_mep['dolar_mep'] = df_mep['Venta'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df_mep['dolar_mep'] = pd.to_numeric(df_mep['dolar_mep'], errors='coerce')
                df_d = pd.merge(df_d, df_mep[['fecha', 'dolar_mep']].dropna(), on='fecha', how='outer')
        except Exception as e: print(f"  -> Error MEP alternativo: {e}")

    # BLUE
    try:
        r = requests.get("https://api.argentinadatos.com/v1/cotizaciones/dolares/blue", headers=HEADERS_BOT, timeout=20, verify=False)
        if r.status_code == 200:
            df_blue = pd.DataFrame(r.json())
            df_blue['fecha'] = pd.to_datetime(df_blue['fecha'])
            if df_d.empty:
                df_d = df_blue[['fecha', 'venta']].rename(columns={'venta': 'dolar_blue'})
            else:
                df_d = pd.merge(df_d, df_blue[['fecha', 'venta']].rename(columns={'venta': 'dolar_blue'}), on='fecha', how='outer')
    except:
        print("  -> ArgentinaDatos bloqueado para Blue. Usando Ámbito...")
        try:
            now = pd.Timestamp.now().strftime('%Y-%m-%d')
            r = requests.get(f"https://mercados.ambito.com//dolar/informal/historico-general/2004-01-01/{now}", headers=HEADERS_BOT, verify=False)
            data = r.json()
            if len(data) > 1:
                df_b = pd.DataFrame(data[1:], columns=data[0])
                df_b['fecha'] = pd.to_datetime(df_b['Fecha'], format='%d-%m-%Y', errors='coerce')
                df_b['dolar_blue'] = df_b['Venta'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df_b['dolar_blue'] = pd.to_numeric(df_b['dolar_blue'], errors='coerce')
                if df_d.empty:
                    df_d = df_b[['fecha', 'dolar_blue']].dropna()
                else:
                    df_d = pd.merge(df_d, df_b[['fecha', 'dolar_blue']].dropna(), on='fecha', how='outer')
        except Exception as e: print(f"  -> Error Blue alternativo: {e}")
        
    return df_d

def fetch_riesgo_pais():
    print("Descargando Riesgo País...")
    try:
        # Intento 1: API ArgentinaDatos
        r = requests.get("https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais", headers=HEADERS_BOT, timeout=20, verify=False)
        if r.status_code == 200:
            df_rp = pd.DataFrame(r.json())
            df_rp['fecha'] = pd.to_datetime(df_rp['fecha'])
            df_rp = df_rp.rename(columns={'valor': 'riesgo_pais'})
            print("  -> Riesgo País obtenido de ArgentinaDatos")
            return df_rp[['fecha', 'riesgo_pais']].sort_values('fecha')
    except:
        pass
        
    print("  -> ArgentinaDatos bloqueado. Intentando por fuente alternativa (Ámbito)...")
    try:
        # Intento 2: Scrapeo directo a la API de Ámbito (Saltea bloqueos de Cloudflare)
        now = pd.Timestamp.now().strftime('%Y-%m-%d')
        url = f"https://mercados.ambito.com//riesgopais/historico-general/2004-01-01/{now}"
        r = requests.get(url, headers=HEADERS_BOT, timeout=20, verify=False)
        if r.status_code == 200:
            data = r.json()
            if len(data) > 1:
                df_rp = pd.DataFrame(data[1:], columns=data[0])
                df_rp['fecha'] = pd.to_datetime(df_rp['Fecha'], format='%d-%m-%Y', errors='coerce')
                # Ámbito trae los números con puntos de miles y comas decimales
                df_rp['riesgo_pais'] = df_rp['Valor'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df_rp['riesgo_pais'] = pd.to_numeric(df_rp['riesgo_pais'], errors='coerce')
                print("  -> Riesgo País obtenido con éxito desde Ámbito")
                return df_rp[['fecha', 'riesgo_pais']].dropna().sort_values('fecha')
    except Exception as e: 
        print(f"  -> Error Riesgo País Ámbito: {e}")
        
    return pd.DataFrame(columns=['fecha', 'riesgo_pais'])

def fetch_us_cpi():
    print("Procesando CPI de EEUU (Inflación en Dólares)...")
    df_cpi = pd.DataFrame(columns=['fecha', 'us_cpi'])
    
    # Búsqueda a prueba de balas para Linux: rastrea cualquier archivo que arranque con "cpiaucsl"
    cpi_path = None
    try:
        for file in os.listdir(BASE_DIR):
            if file.lower().startswith('CPIAUCSL'):
                cpi_path = os.path.join(BASE_DIR, file)
                print(f"  -> Archivo CPI detectado: {file}")
                break
    except Exception as e:
        print(f"  -> Error escaneando carpeta: {e}")

    if cpi_path and os.path.exists(cpi_path):
        try:
            # Detecta inteligentemente si subiste un CSV o un Excel
            if cpi_path.lower().endswith('.csv'):
                df_hist = pd.read_csv(cpi_path)
            else:
                df_hist = pd.read_excel(cpi_path)
            
            # Detectamos cómo se llama la columna de fecha (suele ser DATE u observation_date)
            col_fecha = None
            for col in df_hist.columns:
                if str(col).strip().lower() in ['date', 'observation_date', 'fecha']:
                    col_fecha = col
                    break
            if not col_fecha: col_fecha = df_hist.columns[0]
            
            # Detectamos cómo se llama la columna de valores
            col_valor = None
            for col in df_hist.columns:
                if str(col).strip().upper() == 'CPIAUCSL':
                    col_valor = col
                    break
            if not col_valor: col_valor = df_hist.columns[1]
            
            df_hist['fecha'] = pd.to_datetime(df_hist[col_fecha], errors='coerce')
            df_hist = df_hist.rename(columns={col_valor: 'us_cpi'})
            df_cpi = df_hist[['fecha', 'us_cpi']].dropna(subset=['fecha', 'us_cpi'])
            df_cpi['us_cpi'] = pd.to_numeric(df_cpi['us_cpi'], errors='coerce')
            print(f"  -> ¡Éxito! CPI leído y procesado: {len(df_cpi)} meses.")
        except Exception as e:
            print(f"  -> Error leyendo el archivo local: {e}")
    else:
        print("  -> AVISO: No se encontró ningún archivo CPIAUCSL en el repositorio.")
        
    return df_cpi.sort_values('fecha')

def fetch_tasa_fed():
    print("Descargando Tasa de la FED (EFFR)...")
    now = pd.Timestamp.now()
    start_date = "2004-01-01"
    end_date = now.strftime('%Y-%m-%d')
    url = f"https://markets.newyorkfed.org/api/rates/unsecured/effr/search.json?startDate={start_date}&endDate={end_date}"
    
    try:
        r = requests.get(url, headers=HEADERS_BOT, timeout=20, verify=False)
        
        if r.status_code == 200:
            data = r.json().get('refRates', [])
            if data:
                df = pd.DataFrame(data)
                df = df[['effectiveDate', 'percentRate']].rename(columns={
                    'effectiveDate': 'fecha', 
                    'percentRate': 'tasa_fed'
                })
                
                df['fecha'] = pd.to_datetime(df['fecha'])
                df['tasa_fed'] = pd.to_numeric(df['tasa_fed'], errors='coerce')
                
                df['periodo'] = df['fecha'].dt.to_period('M')
                df_mensual = df.groupby('periodo').last().reset_index()
                df_mensual['fecha'] = df_mensual['periodo'].dt.strftime('%Y-%m')
                
                print(f"  -> ¡Tasa FED descargada! {len(df_mensual)} meses obtenidos.")
                return df_mensual[['fecha', 'tasa_fed']]
    except Exception as e:
        print(f"  -> Error al extraer Tasa FED: {e}")
        
    return pd.DataFrame(columns=['fecha', 'tasa_fed'])

def fetch_bandas_cambiarias():
    print("Procesando Bandas Cambiarias del BCRA...")
    df_final = pd.DataFrame(columns=['fecha', 'banda_inferior', 'banda_superior'])

    try:
        url_excel = "https://www.bcra.gob.ar/archivos/Pdfs/PublicacionesEstadisticas/serie-completa-bandas-cambiarias.xlsx"
        r = requests.get(url_excel, headers=HEADERS_BOT, verify=False, timeout=30)
        temp_bandas = os.path.join(BASE_DIR, 'bandas_temp.xlsx')
        if r.status_code == 200:
            with open(temp_bandas, 'wb') as f: f.write(r.content)
            df_actual = pd.read_excel(temp_bandas, skiprows=6)
            
            col_map = {}
            for c in df_actual.columns:
                c_str = str(c).strip().lower()
                if 'fecha' in c_str: col_map[c] = 'fecha'
                elif 'inferior' in c_str: col_map[c] = 'banda_inferior'
                elif 'superior' in c_str: col_map[c] = 'banda_superior'
            
            df_actual = df_actual.rename(columns=col_map)
            if all(col in df_actual.columns for col in ['fecha', 'banda_inferior', 'banda_superior']):
                df_actual = df_actual[['fecha', 'banda_inferior', 'banda_superior']].dropna(subset=['fecha'])
                df_actual['fecha'] = pd.to_datetime(df_actual['fecha'], errors='coerce')
                for col in ['banda_inferior', 'banda_superior']:
                    df_actual[col] = pd.to_numeric(df_actual[col], errors='coerce')
                
                df_final = pd.concat([df_final, df_actual])
                
            if os.path.exists(temp_bandas): os.remove(temp_bandas)
    except: pass
            
    if not df_final.empty:
        df_final['fecha'] = pd.to_datetime(df_final['fecha'], errors='coerce')
        df_final = df_final.dropna(subset=['fecha']).drop_duplicates(subset=['fecha']).sort_values('fecha')
        return df_final
        
    return pd.DataFrame(columns=['fecha', 'banda_inferior', 'banda_superior'])

def fetch_bcra_history(id_var, nombre, is_daily=False):
    print(f"Descargando {nombre}...")
    df_acumulado = pd.DataFrame()
    offset = 0
    while True:
        try:
            r = requests.get(f"https://api.bcra.gob.ar/estadisticas/v4.0/monetarias/{id_var}?limit=3000&offset={offset}", verify=False, timeout=20)
            if r.status_code != 200: break
            data = r.json().get('results', [{}])[0].get('detalle')
            if not data: break
            df_acumulado = pd.concat([df_acumulado, pd.DataFrame(data)], ignore_index=True)
            if len(data) < 3000 or len(df_acumulado) >= 15000: break
            offset += 3000
        except: break
    if df_acumulado.empty: return pd.DataFrame(columns=['fecha', nombre])
    df_acumulado['fecha'] = pd.to_datetime(df_acumulado['fecha'])
    df_acumulado['valor'] = pd.to_numeric(df_acumulado['valor'], errors='coerce')
    df_acumulado = df_acumulado.rename(columns={'valor': nombre})
    
    if is_daily:
        df_acumulado['fecha'] = df_acumulado['fecha'].dt.strftime('%Y-%m-%d')
        return df_acumulado.groupby('fecha').last().reset_index()[['fecha', nombre]]
    else:
        df_acumulado['periodo'] = df_acumulado['fecha'].dt.to_period('M')
        df_acumulado = df_acumulado.groupby('periodo').last().reset_index()
        df_acumulado['fecha'] = df_acumulado['periodo'].dt.strftime('%Y-%m')
        return df_acumulado[['fecha', nombre]]

print("=== INICIANDO ROBOT BCRA ===")

# --- SISTEMA DE MEMORIA ANTIFALLOS Y LIMPIEZA DE CORRUPTOS ---
json_path = os.path.join(BASE_DIR, 'datos_historicos.json')
df_mensual_old = pd.DataFrame()
df_diario_old = pd.DataFrame()

if os.path.exists(json_path):
    print("-> Cargando datos históricos anteriores para prevenir blancos si falla una API...")
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data_old = json.load(f)
            if 'mensual' in data_old and data_old['mensual']:
                df_mensual_old = pd.DataFrame(data_old['mensual'])
                df_mensual_old['fecha'] = pd.to_datetime(df_mensual_old['fecha'], errors='coerce')
                df_mensual_old = df_mensual_old.dropna(subset=['fecha'])
                df_mensual_old['fecha'] = df_mensual_old['fecha'].dt.strftime('%Y-%m')
                df_mensual_old = df_mensual_old.set_index('fecha')
            if 'diario' in data_old and data_old['diario']:
                df_diario_old = pd.DataFrame(data_old['diario'])
                df_diario_old['fecha'] = pd.to_datetime(df_diario_old['fecha'], errors='coerce')
                df_diario_old = df_diario_old.dropna(subset=['fecha'])
                df_diario_old['fecha'] = df_diario_old['fecha'].dt.strftime('%Y-%m-%d')
                df_diario_old = df_diario_old.set_index('fecha')
    except Exception as e:
        print(f"-> Aviso: No se pudo cargar el JSON previo: {e}")

print("=== CONSTRUYENDO BASE DE DATOS ===")
df_dolares = fetch_dolares_history()
df_itcrm = fetch_itcrm_excel()
df_us_cpi = fetch_us_cpi()
df_fed = fetch_tasa_fed()
df_bandas = fetch_bandas_cambiarias()
df_rp = fetch_riesgo_pais()

df_mensual = pd.DataFrame(columns=['fecha'])

for id_var, nombre in VARS_MENSUAL.items():
    df_temp = fetch_bcra_history(id_var, nombre, is_daily=False)
    if not df_temp.empty: df_mensual = pd.merge(df_mensual, df_temp, on='fecha', how='outer')

if not df_bandas.empty:
    df_bandas_m = df_bandas.copy()
    df_bandas_m['fecha'] = pd.to_datetime(df_bandas_m['fecha'], errors='coerce')
    df_bandas_m = df_bandas_m.dropna(subset=['fecha'])
    df_bandas_m['periodo'] = df_bandas_m['fecha'].dt.to_period('M')
    df_bandas_m = df_bandas_m.groupby('periodo').last().reset_index()
    df_bandas_m['fecha'] = df_bandas_m['periodo'].dt.strftime('%Y-%m')
    df_mensual = pd.merge(df_mensual, df_bandas_m[['fecha', 'banda_inferior', 'banda_superior']], on='fecha', how='outer')

for df_externo in [df_itcrm, df_dolares, df_us_cpi, df_fed]:
    if not df_externo.empty:
        df_ext = df_externo.copy()
        df_ext['fecha'] = pd.to_datetime(df_ext['fecha'], errors='coerce')
        df_ext = df_ext.dropna(subset=['fecha'])
        
        df_ext['periodo'] = df_ext['fecha'].dt.to_period('M')
        df_ext = df_ext.groupby('periodo').last().reset_index()
        df_ext['fecha'] = df_ext['periodo'].dt.strftime('%Y-%m')
        df_mensual = pd.merge(df_mensual, df_ext.drop(columns=['periodo']), on='fecha', how='outer')

if not df_rp.empty:
    df_rp_m = df_rp.copy()
    df_rp_m['periodo'] = df_rp_m['fecha'].dt.to_period('M')
    df_rp_m = df_rp_m.groupby('periodo').last().reset_index()
    df_rp_m['fecha'] = df_rp_m['periodo'].dt.strftime('%Y-%m')
    df_mensual = pd.merge(df_mensual, df_rp_m[['fecha', 'riesgo_pais']], on='fecha', how='outer')

if not df_mensual.empty:
    df_mensual = df_mensual.dropna(subset=['fecha'])
    df_mensual = df_mensual.set_index('fecha')

if not df_mensual_old.empty:
    if not df_mensual.empty:
        df_mensual = df_mensual.combine_first(df_mensual_old)
    else:
        df_mensual = df_mensual_old.copy()
        
if not df_mensual.empty:
    if df_mensual.index.name == 'fecha':
        df_mensual = df_mensual.reset_index()
    df_mensual = df_mensual.dropna(subset=['fecha'])
    df_mensual = df_mensual.sort_values('fecha').ffill()

if len(df_mensual.columns) > 1:
    usd_cols = ['depositos_usd', 'prestamos_usd', 'reservas', 'compra_divisas']
    for col in usd_cols:
        if col in df_mensual.columns:
            df_mensual[col + '_corriente'] = df_mensual[col].copy()
            
    if 'us_cpi' in df_mensual.columns and df_mensual['us_cpi'].notna().any():
        last_cpi = df_mensual['us_cpi'].dropna().iloc[-1]
        df_mensual['us_cpi_index'] = df_mensual['us_cpi'] / last_cpi
        for col in usd_cols:
            if col in df_mensual.columns:
                df_mensual[col] = df_mensual[col + '_corriente'] / df_mensual['us_cpi_index']
    
    if 'ipc_mensual' in df_mensual.columns:
        index_vals = [1.0]
        for i in range(1, len(df_mensual)):
            ipc_val = df_mensual['ipc_mensual'].iloc[i]
            index_vals.append(index_vals[-1] * (1 + ipc_val / 100.0) if pd.notna(ipc_val) else index_vals[-1])
        df_mensual['ipc_index'] = index_vals
        df_mensual['ipc_index'] = df_mensual['ipc_index'] / df_mensual['ipc_index'].iloc[-1]
        
        ars_cols = ['adelantos', 'documentos', 'hipotecarios', 'prendarios', 'personales', 'tarjetas', 
                    'cc', 'ca', 'plazo_fijo', 'base_monetaria', 'm2', 'm2_transaccional', 
                    'circulacion_monetaria', 'billetes_monedas', 'pases_pasivos', 'lefi',
                    'tc_mayorista', 'tc_minorista', 'dolar_mep', 'dolar_blue', 'banda_inferior', 'banda_superior']
        for col in ars_cols:
            if col in df_mensual.columns:
                df_mensual[col + '_corriente'] = df_mensual[col] 
                df_mensual[col] = df_mensual[col + '_corriente'] / df_mensual['ipc_index']
                
    if 'tc_mayorista_corriente' in df_mensual.columns: df_mensual['tc_mayorista_var'] = df_mensual['tc_mayorista_corriente'].pct_change() * 100
    if 'tc_minorista_corriente' in df_mensual.columns:
        if 'dolar_mep_corriente' in df_mensual.columns: df_mensual['brecha_mep'] = ((df_mensual['dolar_mep_corriente'] / df_mensual['tc_minorista_corriente']) - 1) * 100
        if 'dolar_blue_corriente' in df_mensual.columns: df_mensual['brecha_blue'] = ((df_mensual['dolar_blue_corriente'] / df_mensual['tc_minorista_corriente']) - 1) * 100
    if 'rem_interanual' in df_mensual.columns: df_mensual['rem_interanual'] = df_mensual['rem_interanual'].shift(12)
    df_mensual = df_mensual.where(pd.notnull(df_mensual), None).tail(300)

# --- CONSTRUCCIÓN DATASET DIARIO ---
df_diario = pd.DataFrame(columns=['fecha'])

for id_var, nombre in VARS_DIARIO.items():
    df_temp = fetch_bcra_history(id_var, nombre, is_daily=True)
    if not df_temp.empty: 
        df_diario = pd.merge(df_diario, df_temp, on='fecha', how='outer')

# SEGURO ANTI-FALLOS: Nos aseguramos que las columnas existan
for nombre in VARS_DIARIO.values():
    if nombre not in df_diario.columns:
        df_diario[nombre] = float('nan')

if not df_bandas.empty:
    df_bandas_d = df_bandas.copy()
    df_bandas_d['fecha'] = df_bandas_d['fecha'].dt.strftime('%Y-%m-%d')
    df_diario = pd.merge(df_diario, df_bandas_d, on='fecha', how='outer')

if not df_rp.empty:
    df_rp_d = df_rp.copy()
    df_rp_d['fecha'] = df_rp_d['fecha'].dt.strftime('%Y-%m-%d')
    df_diario = pd.merge(df_diario, df_rp_d, on='fecha', how='outer')

if not df_diario.empty:
    df_diario = df_diario.dropna(subset=['fecha'])
    df_diario = df_diario.set_index('fecha')

if not df_diario_old.empty:
    if not df_diario.empty:
        df_diario = df_diario.combine_first(df_diario_old)
    else:
        df_diario = df_diario_old.copy()

if not df_diario.empty:
    if df_diario.index.name == 'fecha':
        df_diario = df_diario.reset_index()
    
    df_diario = df_diario.dropna(subset=['fecha'])
    df_diario = df_diario.sort_values('fecha').ffill().where(pd.notnull(df_diario), None).tail(1000)

with open(json_path, 'w', encoding='utf-8') as f: 
    json.dump({'mensual': df_mensual.to_dict(orient='list') if len(df_mensual.columns) > 1 else {}, 'diario': df_diario.to_dict(orient='list') if len(df_diario.columns) > 1 else {}}, f)
print(f"\n¡ÉXITO ABSOLUTO! Datos guardados en {json_path}")