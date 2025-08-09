# carga_segura_nueva_tabla.py – Carga segura a nueva tabla
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
SQL_TABLE    = "dbo.TransferenciasClaro_Nueva"  # NUEVA TABLA

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
    r = sess.post(API_URL, json=payload, timeout=30)
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

def verificar_tabla():
    """Verifica que la nueva tabla existe"""
    with conectar_sql() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {SQL_TABLE}")
        total = cur.fetchone()[0]
        print(f"{datetime.now()} – 📊 Tabla {SQL_TABLE}: {total} registros existentes")
        return total

def insertar_batch_seguro(df, conn):
    """Inserta datos de forma segura con manejo de duplicados"""
    if df.empty:
        return 0
    
    cols = [
        "InteractionIDKey","Numero","Nombre","Cédula","Time","Usuario",
        "Cola","Categoria","Codigo","Perfil Trasnfer","Ultima Cola","DNIS_LocalID"
    ]
    cols_sql = [f"[{col}]" if " " in col else col for col in cols]
    
    # BULK INSERT con manejo de errores
    data_to_insert = [tuple(row) for row in df[cols].values]
    insert_q = f"INSERT INTO {SQL_TABLE} ({','.join(cols_sql)}) VALUES ({','.join(['?' for _ in cols])})"
    
    cur = conn.cursor()
    insertados = 0
    
    for row in data_to_insert:
        try:
            cur.execute(insert_q, row)
            insertados += 1
        except pyodbc.IntegrityError as e:
            # Duplicado - ignorar silenciosamente
            if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                continue
            else:
                raise e
    
    conn.commit()
    return insertados

def procesar_chunk_30min(ses, fecha, hora_inicio, reintentos=3):
    """Procesa un chunk de 30 minutos con reintentos mejorados"""
    hora_fin = hora_inicio + 0.5  # 30 minutos
    
    # Convertir float a int para formateo correcto
    hora_inicio_int = int(hora_inicio)
    minuto_inicio = int((hora_inicio % 1) * 60)
    hora_fin_int = int(hora_fin)
    minuto_fin = int((hora_fin % 1) * 60)
    
    start = fecha.strftime(f"%Y-%m-%d {hora_inicio_int:02d}:{minuto_inicio:02d}:00")
    end = fecha.strftime(f"%Y-%m-%d {hora_fin_int:02d}:{minuto_fin:02d}:00")
    
    for intento in range(reintentos):
        try:
            inicio = datetime.now()
            df = procesar(consultar_api(ses, start, end))
            tiempo_api = (datetime.now() - inicio).total_seconds()
            
            if not df.empty:
                inicio_insert = datetime.now()
                with conectar_sql() as cn:
                    nuevos = insertar_batch_seguro(df, cn)
                tiempo_insert = (datetime.now() - inicio_insert).total_seconds()
                print(f"{datetime.now()} – ✅ {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}:{minuto_inicio:02d}h: {len(df)} filas → {nuevos} insertadas (API: {tiempo_api:.1f}s, SQL: {tiempo_insert:.1f}s)")
                return nuevos
            else:
                print(f"{datetime.now()} – ℹ️ {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}:{minuto_inicio:02d}h: Sin datos")
                return 0
        except Exception as e:
            if "400" in str(e) and intento < reintentos - 1:
                print(f"{datetime.now()} – ℹ️ {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}:{minuto_inicio:02d}h: Sin datos (API rechazó consulta)")
                return 0
            elif intento < reintentos - 1:
                print(f"{datetime.now()} – ⚠️ Reintento {intento+1}/{reintentos} para {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}:{minuto_inicio:02d}h: {e}")
                time.sleep(5)  # Pausa más larga
            else:
                print(f"{datetime.now()} – ❌ Error final en {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}:{minuto_inicio:02d}h después de {reintentos} intentos: {e}")
                return 0

def procesar_dia_paralelo(ses, fecha_actual):
    """Procesa un día completo en chunks de 30 minutos"""
    print(f"{datetime.now()} – 📅 Procesando {fecha_actual.strftime('%Y-%m-%d')} (48 chunks de 30min)...")
    
    # Crear chunks de 30 minutos
    chunks = []
    for hora in range(24):
        for minuto in [0, 30]:
            hora_float = hora + minuto / 60
            chunks.append((fecha_actual, hora_float))
    
    total_filas = 0
    chunks_exitosos = 0
    chunks_fallidos = 0
    
    # Procesar en paralelo con máximo 6 threads
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(procesar_chunk_30min, ses, fecha, hora) for fecha, hora in chunks]
        
        for future in as_completed(futures):
            resultado = future.result()
            if resultado > 0:
                chunks_exitosos += 1
                total_filas += resultado
            else:
                chunks_fallidos += 1
    
    print(f"{datetime.now()} – 🎯 {fecha_actual.strftime('%Y-%m-%d')}: TOTAL {total_filas} filas (✅ {chunks_exitosos} chunks, ❌ {chunks_fallidos} chunks)")
    return total_filas

def main():
    print(f"{datetime.now()} – 🚀 Iniciando carga SEGURA a nueva tabla")
    
    # 1. Verificar tabla
    registros_iniciales = verificar_tabla()
    
    # 2. Calcular fechas
    fecha_inicio = datetime(2025, 7, 1)
    fecha_fin = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    dias_totales = (fecha_fin - fecha_inicio).days + 1
    
    print(f"{datetime.now()} – 📊 Cargando datos desde {fecha_inicio.strftime('%Y-%m-%d')} hasta {fecha_fin.strftime('%Y-%m-%d')}")
    print(f"{datetime.now()} – 📅 Total días a procesar: {dias_totales}")
    
    # 3. Login
    ses = login()
    
    # 4. Procesar cada día
    fecha_actual = fecha_inicio
    total_filas = 0
    dias_procesados = 0
    
    while fecha_actual <= fecha_fin:
        dias_procesados += 1
        print(f"{datetime.now()} – 📈 Progreso: {dias_procesados}/{dias_totales} días")
        
        filas_dia = procesar_dia_paralelo(ses, fecha_actual)
        total_filas += filas_dia
        
        fecha_actual += timedelta(days=1)
        
        # Pausa entre días
        time.sleep(2)
    
    # 5. Estadísticas finales
    registros_finales = verificar_tabla()
    nuevos_registros = registros_finales - registros_iniciales
    
    print(f"{datetime.now()} – 🎉 Carga SEGURA completada!")
    print(f"   • Días procesados: {dias_procesados}")
    print(f"   • Filas procesadas: {total_filas}")
    print(f"   • Nuevos registros en BD: {nuevos_registros}")
    print(f"   • Duplicados evitados: {total_filas - nuevos_registros}")

if __name__ == "__main__":
    main() 