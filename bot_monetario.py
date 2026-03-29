import requests, pandas as pd, json, warnings, os
warnings.filterwarnings('ignore')

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
    try:
        r = requests.get("https://www.bcra.gob.ar/archivos/Pdfs/PublicacionesEstadisticas/ITCRMSerie.xlsx", headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=30)
        if r.status_code == 200:
            with open('itcrm_temp.xlsx', 'wb') as f: f.write(r.content)
            df = pd.read_excel('itcrm_temp.xlsx', sheet_name=0, skiprows=1)
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
            if os.path.exists('itcrm_temp.xlsx'): os.remove('itcrm_temp.xlsx')
            print("  -> ¡Excel ITCRM procesado con éxito!")
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

print("=== CONSTRUYENDO BASE DE DATOS ===")
df_dolares = fetch_dolares_history()
df_itcrm = fetch_itcrm_excel()
df_mensual = pd.DataFrame(columns=['fecha'])

for id_var, nombre in VARS_MENSUAL.items():
    df_temp = fetch_bcra_history(id_var, nombre, is_daily=False)
    if not df_temp.empty: df_mensual = pd.merge(df_mensual, df_temp, on='fecha', how='outer')

for df_externo in [df_itcrm, df_dolares]:
    if not df_externo.empty:
        df_ext = df_externo.copy()
        df_ext['periodo'] = df_ext['fecha'].dt.to_period('M')
        df_ext = df_ext.groupby('periodo').last().reset_index()
        df_ext['fecha'] = df_ext['periodo'].dt.strftime('%Y-%m')
        df_mensual = pd.merge(df_mensual, df_ext.drop(columns=['periodo']), on='fecha', how='outer')

if len(df_mensual.columns) > 1:
    df_mensual = df_mensual.sort_values('fecha').ffill()
    if 'ipc_mensual' in df_mensual.columns:
        index_vals = [1.0]
        for i in range(1, len(df_mensual)):
            ipc_val = df_mensual['ipc_mensual'].iloc[i]
            index_vals.append(index_vals[-1] * (1 + ipc_val / 100.0) if pd.notna(ipc_val) else index_vals[-1])
        df_mensual['ipc_index'] = index_vals
        df_mensual['ipc_index'] = df_mensual['ipc_index'] / df_mensual['ipc_index'].iloc[-1]
        
        # Agregamos los Dólares a la licuadora deflactora
        ars_cols = ['adelantos', 'documentos', 'hipotecarios', 'prendarios', 'personales', 'tarjetas', 
                    'cc', 'ca', 'plazo_fijo', 'base_monetaria', 'm2', 'm2_transaccional', 
                    'circulacion_monetaria', 'billetes_monedas', 'pases_pasivos', 'lefi',
                    'tc_mayorista', 'tc_minorista', 'dolar_mep', 'dolar_blue']
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
if not df_diario.empty: df_diario = df_diario.sort_values('fecha').ffill().where(pd.notnull(df_diario), None).tail(250)

with open(os.path.join(os.getcwd(), 'datos_historicos.json'), 'w', encoding='utf-8') as f: 
    json.dump({'mensual': df_mensual.to_dict(orient='list') if len(df_mensual.columns) > 1 else {}, 'diario': df_diario.to_dict(orient='list') if len(df_diario.columns) > 1 else {}}, f)
print(f"\n¡ÉXITO ABSOLUTO! Datos guardados en datos_historicos.json")