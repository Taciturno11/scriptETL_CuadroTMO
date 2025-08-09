# carga_masiva_simple.py – Carga masiva SIMPLE Y FUNCIONAL
import requests, pandas as pd, pyodbc, time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ───────── Configuración ─────────
LOGIN_URL = "http://10.106.17.135:3000/login"
API_URL   = "http://10.106.17.135:3000/api/ds/query"

USER = "CONRMOLJ"
PWD  = "Claro2024"

SQL_SERVER   = "172.16.248.48"
SQL_DATABASE = "Partner"
SQL_USER     = "anubis"
SQL_PASSWORD = "Tg7#kPz9@rLt2025"
SQL_TABLE    = "dbo.TransferenciasClaro"

# ───────── Consulta ─────────
RAW_SQL_TEMPLATE = """
SELECT
  CONCAT('IDK ', IW.InteractionIDKey)   AS InteractionIDKey,
  I.RemoteID                            AS Numero,
  I.RemoteName                          AS Nombre,
  ICA.CustomString4                     AS [Cédula],
  IW.WrapupStartDateTimeUTC             AS [Time],
  IW.UserID                             AS Usuario,
  IW.WorkgroupID                        AS Cola,
  IW.WrapupCategory                     AS Categoria,
  IW.WrapupCode                         AS Codigo,
  ICA.CustomString3                     AS [Perfil Trasnfer.],
  LastAssignedWorkgroupID               AS [Ultima Cola],
  I.DNIS_LocalID                        AS DNIS_LocalID
FROM   [I3_IC_2020].[dbo].[InteractionWrapup] IW
LEFT   JOIN [I3_IC_2020].[dbo].[InteractionSummary]          I   ON IW.InteractionIDKey = I.InteractionIDKey
LEFT   JOIN [I3_IC_2020].[dbo].[InteractionCustomAttributes] ICA ON IW.InteractionIDKey = ICA.InteractionIDKey
WHERE  IW.WrapupStartDateTimeUTC BETWEEN '{start}' AND '{end}';
"""

# ───────── Funciones ─────────

def login():
    s = requests.Session()
    s.post(LOGIN_URL, json={"user": USER, "password": PWD}, timeout=30).raise_for_status()
    print(f"{datetime.now()} – ✅ Login OK")
    return s

def consultar_api(sess, start, end):
    sql = RAW_SQL_TEMPLATE.format(start=start, end=end)
    payload = {
        "queries": [{
            "refId": "A",
            "datasource": {"type": "mssql", "uid": "PZT_sj4Gz", "database": "I3_IC_2020"},
            "rawSql": sql,
            "format": "table"
        }],
        "range": {"from": start, "to": end}
    }
    r = sess.post(API_URL, json=payload, timeout=15)
    r.raise_for_status()
    return r.json()

def procesar(js):
    frame = js["results"]["A"]["frames"][0]
    cols  = [f["name"] for f in frame["schema"]["fields"]]
    df    = pd.DataFrame(list(zip(*frame["data"]["values"])), columns=cols)

    # Mapear a nombres de destino
    df = df.rename(columns={
        "Perfil Trasnfer.": "Perfil Trasnfer",
        "DNIS_Loca": "DNIS_LocalID"
    })

    df["Time"] = (
        pd.to_datetime(df["Time"].astype(float), unit="ms", utc=True)
          .dt.tz_convert("America/Lima").dt.tz_localize(None)
    )
    return df

def conectar_sql():
    return pyodbc.connect(
        "DRIVER={SQL Server};"
        f"SERVER={SQL_SERVER},1433;DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASSWORD};TrustServerCertificate=yes;"
    )

def limpiar_tabla():
    with conectar_sql() as conn:
        cur = conn.cursor()
        print(f"{datetime.now()} – 🗑️ Limpiando tabla {SQL_TABLE}...")
        cur.execute(f"DELETE FROM {SQL_TABLE}")
        conn.commit()
        print(f"{datetime.now()} – ✅ Tabla limpiada")

def insertar_batch(df, conn):
    if df.empty:
        return 0
    cols = [
        "InteractionIDKey","Numero","Nombre","Cédula","Time","Usuario",
        "Cola","Categoria","Codigo","Perfil Trasnfer","Ultima Cola","DNIS_LocalID"
    ]
    cols_sql = [f"[{col}]" if " " in col else col for col in cols]
    
    # BULK INSERT - Máximo rendimiento
    data_to_insert = [tuple(row) for row in df[cols].values]
    insert_q = f"INSERT INTO {SQL_TABLE} ({','.join(cols_sql)}) VALUES ({','.join(['?' for _ in cols])})"
    
    cur = conn.cursor()
    cur.executemany(insert_q, data_to_insert)
    conn.commit()
    return len(data_to_insert)

def procesar_chunk_30min(ses, fecha, hora_inicio, reintentos=2):
    """Procesa un chunk de 30 minutos con reintentos"""
    hora_fin = hora_inicio + 0.5  # 30 minutos
    
    # Convertir float a int para formateo correcto
    hora_inicio_int = int(hora_inicio)
    hora_fin_int = int(hora_fin)
    
    start = fecha.strftime(f"%Y-%m-%d {hora_inicio_int:02d}:00:00")
    end = fecha.strftime(f"%Y-%m-%d {hora_fin_int:02d}:00:00")
    
    for intento in range(reintentos):
        try:
            inicio = datetime.now()
            df = procesar(consultar_api(ses, start, end))
            tiempo_api = (datetime.now() - inicio).total_seconds()
            
            if not df.empty:
                inicio_insert = datetime.now()
                with conectar_sql() as cn:
                    nuevos = insertar_batch(df, cn)
                tiempo_insert = (datetime.now() - inicio_insert).total_seconds()
                print(f"{datetime.now()} – ✅ {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}h: {len(df)} filas → {nuevos} insertadas (API: {tiempo_api:.1f}s, SQL: {tiempo_insert:.1f}s)")
                return len(df)
            else:
                print(f"{datetime.now()} – ℹ️ {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}h: Sin datos")
                return 0
        except Exception as e:
            if intento < reintentos - 1:
                print(f"{datetime.now()} – ⚠️ Reintento {intento+1}/{reintentos} para {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}h: {e}")
                time.sleep(2)  # Pausa antes del reintento
            else:
                print(f"{datetime.now()} – ❌ Error final en {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}h después de {reintentos} intentos: {e}")
                return 0

def procesar_dia_paralelo(ses, fecha):
    """Procesa un día completo usando 48 chunks de 30 minutos en paralelo"""
    print(f"{datetime.now()} – 📅 Procesando {fecha.strftime('%Y-%m-%d')} (48 chunks de 30min)...")
    
    # Crear 48 chunks de 30 minutos (00:00, 00:30, 01:00, 01:30, etc.)
    chunks = []
    for hora in range(24):
        chunks.append(hora)  # 00:00
        chunks.append(hora + 0.5)  # 00:30
    
    total_filas = 0
    chunks_exitosos = 0
    chunks_fallidos = 0
    
    # Procesar en paralelo con máximo 8 threads
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(procesar_chunk_30min, ses, fecha, hora) for hora in chunks]
        
        for future in as_completed(futures):
            resultado = future.result()
            if resultado > 0:
                chunks_exitosos += 1
                total_filas += resultado
            else:
                chunks_fallidos += 1
    
    print(f"{datetime.now()} – 🎯 {fecha.strftime('%Y-%m-%d')}: TOTAL {total_filas} filas (✅ {chunks_exitosos} chunks, ❌ {chunks_fallidos} chunks)")
    return total_filas

def main():
    print(f"{datetime.now()} – 🚀 Iniciando carga masiva SIMPLE Y FUNCIONAL")
    
    # 1. Limpiar tabla
    limpiar_tabla()
    
    # 2. Login
    ses = login()
    
    # 3. Procesar día por día desde 1 Julio hasta hoy
    fecha_inicio = datetime(2025, 7, 1)  # 1 Julio 2025
    fecha_fin = datetime.now()
    
    total_dias = (fecha_fin - fecha_inicio).days + 1
    print(f"{datetime.now()} – 📊 Procesando {total_dias} días ({fecha_inicio.strftime('%Y-%m-%d')} → {fecha_fin.strftime('%Y-%m-%d')})")
    
    fecha_actual = fecha_inicio
    contador = 0
    total_global = 0
    
    while fecha_actual <= fecha_fin:
        contador += 1
        print(f"{datetime.now()} – 📈 Progreso: {contador}/{total_dias} días")
        
        filas_dia = procesar_dia_paralelo(ses, fecha_actual)
        total_global += filas_dia
        
        fecha_actual += timedelta(days=1)
    
    print(f"{datetime.now()} – 🎉 Carga masiva completada! Total: {total_global} filas")

if __name__ == "__main__":
    main() 