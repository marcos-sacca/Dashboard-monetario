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

def fetch_itcrm_excel():
    print("Descargando ITCRM y Bilaterales (Scrapeando Excel BCRA)...")
    temp_path = os.path.join(BASE_DIR, 'itcrm_temp.xlsx')
    try:
        r = requests.get("https://www.bcra.gob.ar/archivos/Pdfs/PublicacionesEstadisticas/ITCRMSerie.xlsx", headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=30)
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
    try:
        r = requests.get("https://api.argentinadatos.com/v1/cotizaciones/dolares/bolsa", timeout=20)
        if r.status_code == 200:
            df_mep = pd.DataFrame(r.json())
            df_mep['fecha'] = pd.to_datetime(df_mep['fecha'])
            df_d = pd.merge(df_d, df_mep[['fecha', 'venta']].rename(columns={'venta': 'dolar_mep'}), on='fecha', how='outer')
    except Exception as e: print(f"  -> Error MEP: {e}")
    try:
        r = requests.get("https://api.argentinadatos.com/v1/cotizaciones/dolares/blue", timeout=20)
        if r.status_code == 200:
            df_blue = pd.DataFrame(r.json())
            df_blue['fecha'] = pd.to_datetime(df_blue['fecha'])
            df_d = pd.merge(df_d, df_blue[['fecha', 'venta']].rename(columns={'venta': 'dolar_blue'}), on='fecha', how='outer')
    except Exception as e: print(f"  -> Error Blue: {e}")
    return df_d

def fetch_us_cpi():
    print("Procesando CPI de EEUU (Inflación en Dólares)...")
    df_cpi = pd.DataFrame(columns=['fecha', 'us_cpi'])
    
    cpi_path = r"C:\Users\Sofia\Downloads\CPIAUCSL.csv"
            
    if os.path.exists(cpi_path):
        try:
            df_hist = pd.read_csv(cpi_path)
            df_hist['fecha'] = pd.to_datetime(df_hist['observation_date'], errors='coerce')
            df_hist = df_hist.rename(columns={'CPIAUCSL': 'us_cpi'})
            df_cpi = df_hist[['fecha', 'us_cpi']].dropna(subset=['fecha', 'us_cpi'])
        except Exception as e:
            print(f"  -> Error leyendo archivo CPI: {e}")

    df_cpi['us_cpi'] = pd.to_numeric(df_cpi['us_cpi'], errors='coerce')

    try:
        api_key = 'guest:guest' 
        url = f"https://api.tradingeconomics.com/country/united states?c={api_key}&f=json"
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            cpi_item = next((item for item in data if item.get('Category') == 'Consumer Price Index CPI'), None)
            if cpi_item:
                last_date = pd.to_datetime(cpi_item.get('LatestValueDate'))
                last_val = float(cpi_item.get('LatestValue'))
                if df_cpi.empty or last_date > df_cpi['fecha'].max():
                    nuevo_registro = pd.DataFrame([{'fecha': last_date, 'us_cpi': last_val}])
                    df_cpi = pd.concat([df_cpi, nuevo_registro], ignore_index=True)
    except Exception as e:
        print(f"  -> Error consultando TradingEconomics: {e}")
        
    return df_cpi.sort_values('fecha')

def fetch_tasa_fed():
    print("Descargando Tasa de la FED (EFFR)...")
    now = pd.Timestamp.now()
    start_date = "2004-01-01"
    end_date = now.strftime('%Y-%m-%d')
    url = f"https://markets.newyorkfed.org/api/rates/unsecured/effr/search.json?startDate={start_date}&endDate={end_date}"
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=20)
        
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
    
    archivo_2025 = r"C:\Users\Sofia\Downloads\serie-completa-bandas-cambiarias-2025.xlsx"
            
    if os.path.exists(archivo_2025):
        try:
            df_raw = pd.read_excel(archivo_2025, header=None, nrows=20)
            header_idx = 0
            for i, row in df_raw.iterrows():
                row_str = " ".join([str(x).lower() for x in row.values])
                if 'fecha' in row_str and ('inferior' in row_str or 'superior' in row_str):
                    header_idx = i
                    break
            
            df_25 = pd.read_excel(archivo_2025, header=header_idx)
            col_map = {}
            for c in df_25.columns:
                c_str = str(c).strip().lower()
                if 'fecha' in c_str: col_map[c] = 'fecha'
                elif 'inferior' in c_str: col_map[c] = 'banda_inferior'
                elif 'superior' in c_str: col_map[c] = 'banda_superior'
            
            df_25 = df_25.rename(columns=col_map)
            if 'fecha' in df_25.columns:
                df_25 = df_25[['fecha', 'banda_inferior', 'banda_superior']].dropna(subset=['fecha'])
                df_25['fecha'] = pd.to_datetime(df_25['fecha'], errors='coerce')
                for col in ['banda_inferior', 'banda_superior']:
                    if col in df_25.columns:
                        if df_25[col].dtype == 'object':
                            df_25[col] = df_25[col].astype(str).str.replace(',', '.', regex=False)
                        df_25[col] = pd.to_numeric(df_25[col], errors='coerce')
                
                df_final = pd.concat([df_final, df_25])
        except: pass

    try:
        url_excel = "https://www.bcra.gob.ar/archivos/Pdfs/PublicacionesEstadisticas/serie-completa-bandas-cambiarias.xlsx"
        r = requests.get(url_excel, headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=30)
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

def fetch_macro_gob_ar():
    print("Descargando Macroeconomía Oficial (Datos.gob.ar)...")
    
    series = {
        "emae": "143.3_NO_PR_2004_A_31", 
        "desempleo": "42.3_EPH_PUNTUATAL_0_M_30" 
    }
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    df_final = pd.DataFrame(columns=['fecha'])
    
    for nombre, id_serie in series.items():
        print(f"  -> Buscando {nombre.upper()}...")
        url = f"https://apis.datos.gob.ar/series/api/series?ids={id_serie}&format=json&limit=5000"
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code == 200:
                data = r.json().get('data', [])
                if data:
                    df_temp = pd.DataFrame(data, columns=['fecha', nombre])
                    df_temp['fecha'] = pd.to_datetime(df_temp['fecha'], errors='coerce')
                    df_temp[nombre] = pd.to_numeric(df_temp[nombre], errors='coerce')
                    
                    df_temp['periodo'] = df_temp['fecha'].dt.to_period('M')
                    df_temp = df_temp.groupby('periodo').last().reset_index()
                    df_temp['fecha'] = df_temp['periodo'].dt.strftime('%Y-%m')
                    df_temp = df_temp.drop(columns=['periodo'])
                    
                    if df_final.empty:
                        df_final = df_temp
                    else:
                        df_final = pd.merge(df_final, df_temp, on='fecha', how='outer')
                    print(f"     [OK] {len(df_temp)} registros descargados.")
            time.sleep(2) 
        except Exception as e:
            print(f"     [FALLA] Conexión: {e}")
            time.sleep(2)
            
    if not df_final.empty:
        df_final['fecha'] = pd.to_datetime(df_final['fecha'])
        df_final = df_final.sort_values('fecha').reset_index(drop=True)
        
        if 'desempleo' in df_final.columns:
            df_final['desempleo'] = df_final['desempleo'].interpolate(method='linear')
            
        df_final = df_final.ffill()
        df_final['fecha'] = df_final['fecha'].dt.strftime('%Y-%m')
        return df_final
        
    return pd.DataFrame(columns=['fecha', 'emae', 'desempleo'])

print("=== CONSTRUYENDO BASE DE DATOS ===")
df_dolares = fetch_dolares_history()
df_itcrm = fetch_itcrm_excel()
df_us_cpi = fetch_us_cpi()
df_fed = fetch_tasa_fed()
df_bandas = fetch_bandas_cambiarias()

df_macro = fetch_macro_gob_ar()

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

for df_externo in [df_itcrm, df_dolares, df_us_cpi, df_fed, df_macro]:
    if not df_externo.empty:
        df_ext = df_externo.copy()
        df_ext['fecha'] = pd.to_datetime(df_ext['fecha'], errors='coerce')
        df_ext = df_ext.dropna(subset=['fecha'])
        
        df_ext['periodo'] = df_ext['fecha'].dt.to_period('M')
        df_ext = df_ext.groupby('periodo').last().reset_index()
        df_ext['fecha'] = df_ext['periodo'].dt.strftime('%Y-%m')
        df_mensual = pd.merge(df_mensual, df_ext.drop(columns=['periodo']), on='fecha', how='outer')

if len(df_mensual.columns) > 1:
    df_mensual = df_mensual.sort_values('fecha').ffill()
    
    usd_cols = ['depositos_usd', 'prestamos_usd', 'reservas', 'compra_divisas']
    for col in usd_cols:
        if col in df_mensual.columns:
            df_mensual[col + '_corriente'] = df_mensual[col].copy()
            
    if 'us_cpi' in df_mensual.columns and df_mensual['us_cpi'].notna().any():
        last_cpi = df_mensual['us_cpi'].dropna().iloc[-1]
        df_mensual['us_cpi_index'] = df_mensual['us_cpi'] / last_cpi
        for col in usd_cols:
            if col in df_mensual.columns:
                df_mensual[col] = df_mensual[col] / df_mensual['us_cpi_index']
    
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
                df_mensual[col] = df_mensual[col] / df_mensual['ipc_index']
                
    if 'tc_mayorista_corriente' in df_mensual.columns: df_mensual['tc_mayorista_var'] = df_mensual['tc_mayorista_corriente'].pct_change() * 100
    if 'tc_minorista_corriente' in df_mensual.columns:
        if 'dolar_mep_corriente' in df_mensual.columns: df_mensual['brecha_mep'] = ((df_mensual['dolar_mep_corriente'] / df_mensual['tc_minorista_corriente']) - 1) * 100
        if 'dolar_blue_corriente' in df_mensual.columns: df_mensual['brecha_blue'] = ((df_mensual['dolar_blue_corriente'] / df_mensual['tc_minorista_corriente']) - 1) * 100
    if 'rem_interanual' in df_mensual.columns: df_mensual['rem_interanual'] = df_mensual['rem_interanual'].shift(12)
    df_mensual = df_mensual.where(pd.notnull(df_mensual), None).tail(240)

df_diario = pd.DataFrame(columns=['fecha'])
for id_var, nombre in VARS_DIARIO.items():
    df_temp = fetch_bcra_history(id_var, nombre, is_daily=True)
    if not df_temp.empty: df_diario = pd.merge(df_diario, df_temp, on='fecha', how='outer')

if not df_bandas.empty:
    df_bandas_d = df_bandas.copy()
    df_bandas_d['fecha'] = df_bandas_d['fecha'].dt.strftime('%Y-%m-%d')
    df_diario = pd.merge(df_diario, df_bandas_d, on='fecha', how='outer')

if not df_diario.empty: 
    df_diario = df_diario.sort_values('fecha').ffill().where(pd.notnull(df_diario), None).tail(250)

json_path = os.path.join(BASE_DIR, 'datos_historicos.json')
with open(json_path, 'w', encoding='utf-8') as f: 
    json.dump({'mensual': df_mensual.to_dict(orient='list') if len(df_mensual.columns) > 1 else {}, 'diario': df_diario.to_dict(orient='list') if len(df_diario.columns) > 1 else {}}, f)
print(f"\n¡ÉXITO ABSOLUTO! Datos guardados en {json_path}")