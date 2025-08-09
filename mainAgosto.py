# cargar_datos_desde_agosto.py
import requests, pandas as pd, pyodbc, pytz, time
from datetime import datetime, timedelta

# ───────── CONFIGURACIÓN ────────────────────────────────────────────────
LOGIN_URL    = "http://10.106.17.135:3000/login"
API_URL      = "http://10.106.17.135:3000/api/ds/query"

USUARIO      = "CONRMOLJ"
CLAVE        = "Claro2024"

SQL_SERVER   = "172.16.248.48"
SQL_DATABASE = "Partner"
SQL_USER     = "anubis"
SQL_PASSWORD = "Tg7#kPz9@rLt2025"
SQL_TABLE    = "Cuadro_TMO2"

# Zonas horarias
TZ_LIMA = pytz.timezone("America/Lima")
TZ_UTC  = pytz.utc

# ── CONSTANTES ──────────────────────────────────────────────────────────
ALMOHADA_HORAS = 2
FECHA_INICIO   = datetime(2025, 8, 1, 0, 0, 0, tzinfo=TZ_LIMA)  # Exactamente 00:00:00 del 1 de agosto

# Colas a consultar
COLAS = [
    "ACC_InbVent_CrossHogar",
    "ACC_InbVentHogar", 
    "ACC_InbventOC",
    "ACC_Renovinb",
    "CAT_InbCrossHogar",
    "CAT_InbVentHogar",
    "CAT_RenovInb",
    "CCC_InbventOC",
    "PARTNER_InbCrossHogar",
    "PARTNER_InbVentHogar",
    "PARTNER_InbventOC",
    "PARTNER_RenovInb"
]

# Plantilla SQL con placeholder {cola}
RAW_SQL_TEMPLATE = """
SELECT
    DATEADD(HOUR, DATEDIFF(HOUR, 0, dIntervalStartUTC), 0) as time,
    [cName],
    [cReportGroup],
    SUM(nAbandonedAcd)+SUM(nAnsweredAcd) AS Recibidas,
    SUM(nAnsweredAcd) as Respondidas,
    SUM(nAbandonedAcd) as Abandonadas,
    SUM(nAbandonAcdSvcLvl1) as 'Abandonadas 5s',
    CASE WHEN SUM(nAnsweredAcd)=0 THEN NULL ELSE SUM(tTalkAcd)/sum(nAnsweredAcd) END as 'TMO s tHablado/int ',
    CASE WHEN SUM(tTalkAcd)=0 THEN NULL ELSE ROUND(1.00*SUM(tHoldAcd)/SUM(tTalkAcd)*100,2) END as '% Hold',
    CASE WHEN SUM(nAnsweredAcd)=0 THEN NULL ELSE SUM(tAnsweredAcd)/SUM(nAnsweredAcd) END as 'TME Respondida',
    CASE WHEN SUM(nAbandonedAcd)=0 THEN NULL ELSE SUM(tAbandonedAcd)/SUM(nAbandonedAcd) END as 'TME Abandonada',
    ROUND(1.00*SUM(tAgentAvailable)/3600,2) as 'Tiempo Disponible H',
    ROUND(1.00*(SUM(tAgentOnOtherAcdCall)+SUM(tAgentOnAcdCall))/3600,2) as 'Tiempo Hablado  H',
    ROUND(1.00*SUM(tAgentDnd)/3600,2) as 'Tiempo Recarga  H',
    ROUND(1.00*SUM(tAgentInAcw)/3600,2) as 'Tiempo ACW  H',
    ROUND(1.00*(SUM(tAgentOnNonAcdCall)+SUM(tAgentNotAvailable))/3600,2) as 'Tiempo No Disponible  H',
    ROUND(1.00*SUM(tAgentLoggedIn)/3600,2) as 'Tiempo Total LoggedIn',
    ROUND(1.00*(SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))/3600,2) as 'Hora ACD',
    CASE WHEN (SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))=0 THEN NULL ELSE ROUND(1.00*SUM(tAgentAvailable)/(SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))*100,2) END as '% Disponible',
    CASE WHEN (SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))=0 THEN NULL ELSE ROUND(1.00*(SUM(tAgentOnOtherAcdCall)+SUM(tAgentOnAcdCall))/(SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))*100,2) END as '% Hablado',
    CASE WHEN (SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))=0 THEN NULL ELSE ROUND(1.00*SUM(tAgentDnd)/(SUM(tAgentAvailable)+SUM(tAgentOnOtherAcdCall+tAgentOnAcdCall)+SUM(tAgentDnd))*100,2) END as '% Recarga',
    sum(nInternToExternCalls) - sum(nInternToExternAcdCalls) as 'Int. Salientes manuales',
    ROUND(1.0*(sum(tInternToExternCalls) - sum(tInternToExternAcdCalls))/3600,2) as 'Tiempo en int. salientes manuales (H)',
    CASE WHEN (sum(nInternToExternCalls) - sum(nInternToExternAcdCalls))=0 THEN NULL ELSE (sum(tInternToExternCalls) - sum(tInternToExternAcdCalls))/(sum(nInternToExternCalls) - sum(nInternToExternAcdCalls)) END AS 'TMO s Int. Salientes manuales'
FROM [I3_IC_2020].[dbo].[IAgentQueueStats]
WHERE
    dIntervalStartUTC BETWEEN '{start}' AND '{end}'
    AND cHKey3 ='*'
    AND cHKey4 ='*'
    AND cReportGroup = '{cola}'
    AND cName IN ('ACCBBALC','ACCCSANP','ACCCVARD','ACCGSUAS','ACCJSALL','ACCMAQUIA','ACCPANAD','ACCVALVT','NTGACHA5','ntgasolp','ntgavego','NTGAVIVR','NTGBGAMU','NTGDGUIC','ntgdpuln','ntgdramv','NTGDSUAL','ntgetoas','ntgfsanp','NTGJAGUN','NTGJAMIZ','NTGJCALT','ntgkcaia','NTGKLLAV','NTGLFIET','NTGLRIOA','NTGNALBI','ntgnfigc','NTGVGARL','NTGWJACB','OVGELOPG','OVGLMONB','OVGRMILA','PSGAVILT','PSGCCORM','PSGCRODT','PSGCZARR','PSGEABAP','PSGEGARR','PSGFCCOQ','PSGJMORJ','PSGJSUYS','PSGLCHIQ','PSGLPOMH','PSGLSANG','PSGMVILS','PSGNGERC','PSGSVICC','PSGYMONU','PSGGPERL')
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, dIntervalStartUTC), 0), cName, cReportGroup
ORDER BY DATEADD(HOUR, DATEDIFF(HOUR, 0, dIntervalStartUTC), 0), cName ASC
"""

def conectar_sql():
    return pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={SQL_SERVER},1433;DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASSWORD};TrustServerCertificate=yes;"
    )

def ultima_fecha_registrada_por_cola(cola):
    """Obtiene la última fecha registrada para una cola específica"""
    with conectar_sql() as cnx:
        cursor = cnx.cursor()
        cursor.execute(f"SELECT MAX(time) FROM {SQL_TABLE} WHERE cReportGroup = ?", (cola,))
        ts = cursor.fetchone()[0]
    if ts is None:
        return FECHA_INICIO
    if ts.tzinfo is None:
        ts = TZ_LIMA.localize(ts)
    else:
        ts = ts.astimezone(TZ_LIMA)
    return ts

def login():
    s = requests.Session()
    s.post(LOGIN_URL, json={"user": USUARIO, "password": CLAVE},
           headers={"Content-Type": "application/json"}).raise_for_status()
    print(f"{datetime.now()} – ✅ Login exitoso")
    return s

def consultar_api(sess, start, end, raw_sql):
    payload = {
        "queries": [{
            "refId": "A",
            "datasource": {"type": "mssql", "uid": "PZT_sj4Gz"},
            "rawSql": raw_sql,
            "format": "table"
        }],
        "range": {"from": start, "to": end}
    }
    r = sess.post(API_URL, json=payload); r.raise_for_status()
    return r.json()

def procesar_datos(j):
    frame = j["results"]["A"]["frames"][0]
    cols  = [f["name"] for f in frame["schema"]["fields"]]
    df    = pd.DataFrame(list(zip(*frame["data"]["values"])), columns=cols)
    df["time"] = (pd.to_datetime(df["time"].astype(float), unit="ms", utc=True)
                     .dt.tz_convert(TZ_LIMA).dt.tz_localize(None))
    
    # Limpiar valores nulos - convertir strings vacíos y 'NULL' a None
    for col in df.columns:
        if col != 'time':  # No tocar la columna time
            df[col] = df[col].replace(['', 'NULL', 'null'], None)
    
    return df

def insertar_datos(df):
    if df.empty:
        return 0, 0
    with conectar_sql() as cnx:
        cur = cnx.cursor()
        
        # Consulta para verificar duplicados manualmente
        verificar_duplicado = f"""
        SELECT COUNT(*) 
        FROM {SQL_TABLE} 
        WHERE time = ? AND cName = ? AND cReportGroup = ?
        """
        
        insert = f"""
          INSERT INTO {SQL_TABLE} (
            time, cName, cReportGroup, Recibidas, Respondidas, Abandonadas, 
            [Abandonadas 5s], [TMO s tHablado/int ], [% Hold], [TME Respondida], 
            [TME Abandonada], [Tiempo Disponible H], [Tiempo Hablado  H], 
            [Tiempo Recarga  H], [Tiempo ACW  H], [Tiempo No Disponible  H], 
            [Tiempo Total LoggedIn], [Hora ACD], [% Disponible], [% Hablado], 
            [% Recarga], [Int. Salientes manuales], [Tiempo en int. salientes manuales (H)], 
            [TMO s Int. Salientes manuales], fechaCarga
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """
        nuevos = dup = 0
        for row in df.itertuples(index=False):
            try:
                # Convertir valores vacíos o nulos a None
                valores = []
                for value in row:
                    if pd.isna(value) or value == '' or value == 'NULL' or value == 'null' or str(value).strip() == '':
                        valores.append(None)
                    else:
                        valores.append(value)
                
                # Verificar si ya existe un registro con la misma clave (time, cName, cReportGroup)
                valores_clave = valores[:3]  # time, cName, cReportGroup
                cur.execute(verificar_duplicado, valores_clave)
                existe = cur.fetchone()[0] > 0
                
                if existe:
                    # Registro duplicado - continuar con el siguiente
                    dup += 1
                else:
                    # Registro nuevo - insertarlo
                    cur.execute(insert, valores)
                    nuevos += 1
                    
            except Exception as e:
                print(f"{datetime.now()} – ⚠️ Error insertando fila: {e}")
                dup += 1
        cnx.commit()
    return nuevos, dup

def calcular_rango_por_cola(cola):
    """Calcula el rango de fechas para una cola específica"""
    ahora_local  = datetime.now(TZ_LIMA)
    ult          = ultima_fecha_registrada_por_cola(cola)
    
    # Si no hay datos o la última fecha es muy antigua, usar FECHA_INICIO
    if ult == FECHA_INICIO:
        inicio_local = FECHA_INICIO
    else:
        # Usar la última fecha registrada como punto de partida
        inicio_local = ult - timedelta(hours=ALMOHADA_HORAS)
    
    start_utc    = inicio_local.astimezone(TZ_UTC).strftime("%Y-%m-%d %H:%M:%S")
    end_utc      = ahora_local.astimezone(TZ_UTC).strftime("%Y-%m-%d %H:%M:%S")
    return start_utc, end_utc

def debug_agentes_procesados(df, cola):
    """Función de debug para verificar qué agentes están siendo procesados"""
    if not df.empty:
        agentes_en_df = df['cName'].unique().tolist()
        print(f"{datetime.now()} – 🔍 DEBUG [{cola}]: Agentes encontrados: {agentes_en_df}")
        
        # Verificar si PSGGPERL está en la lista
        if 'PSGGPERL' in agentes_en_df:
            print(f"{datetime.now()} – ✅ PSGGPERL encontrado en {cola}")
        else:
            print(f"{datetime.now()} – ⚠️ PSGGPERL NO encontrado en {cola}")
    else:
        print(f"{datetime.now()} – ⚠️ DEBUG [{cola}]: DataFrame vacío")

def ciclo(cola):
    start, end = calcular_rango_por_cola(cola)
    print(f"{datetime.now()} – 📥 Consulta: {start} → {end} (UTC) | Cola: {cola}")
    sess    = login()
    raw_sql = RAW_SQL_TEMPLATE.format(start=start, end=end, cola=cola)
    df      = procesar_datos(consultar_api(sess, start, end, raw_sql))
    
    # Debug: verificar agentes procesados
    debug_agentes_procesados(df, cola)
    
    nuevos, dup = insertar_datos(df)
    print(f"{datetime.now()} – 📊 [{cola}] Total:{len(df)} | 🆕 {nuevos} | ⚠️ Dup {dup}")

def proxima_ejecucion():
    """Calcula la próxima ejecución (cada hora)"""
    while True:
        now = datetime.now(TZ_LIMA)
        # Calcular la próxima hora exacta
        proxima_hora = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        espera = (proxima_hora - now).total_seconds()
        
        print(f"{datetime.now()} – ⏳ Esperando {int(espera)} segundos hasta {proxima_hora.strftime('%Y-%m-%d %H:%M:%S')}")
        time.sleep(espera)
        yield

def ejecutar_ciclo_completo():
    """Ejecuta un ciclo completo para todas las colas"""
    print(f"{datetime.now()} – 🚀 Iniciando ciclo de procesamiento")
    total_nuevos = 0
    total_dups = 0
    
    for cola in COLAS:
        try:
            ciclo(cola)
            print(f"{datetime.now()} – ✅ Procesamiento completado ({cola})")
        except Exception as e:
            print(f"{datetime.now()} – ❌ Error en {cola}: {e}")
    
    print(f"{datetime.now()} – 🎯 Ciclo completado")

if __name__ == "__main__":
    print(f"{datetime.now()} – 🚀 Iniciando ETL automático cada hora")
    print(f"{datetime.now()} – 📅 Procesando datos desde: {FECHA_INICIO.strftime('%Y-%m-%d %H:%M:%S')} hasta la actualidad")
    print(f"{datetime.now()} – 🎯 Objetivo: Cargar datos NUEVOS de las {len(COLAS)} colas cada hora")
    
    # Ejecutar inmediatamente la primera vez
    ejecutar_ciclo_completo()
    
    # Luego ejecutar cada hora
    for _ in proxima_ejecucion():
        ejecutar_ciclo_completo() 