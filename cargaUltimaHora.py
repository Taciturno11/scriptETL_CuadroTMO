# analisis_ultima_hora.py – Análisis de última hora sin duplicados
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

def obtener_ultimo_registro():
    """Obtiene el último registro de la base de datos"""
    with conectar_sql() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT TOP 1 [Time] FROM {SQL_TABLE} ORDER BY [Time] DESC")
        resultado = cur.fetchone()
        if resultado:
            return resultado[0]
        else:
            # Si no hay registros, usar 1 hora atrás desde ahora
            return datetime.now() - timedelta(hours=1)

def obtener_registros_existentes(fecha_inicio, fecha_fin):
    """Obtiene los registros existentes en el rango de tiempo para evitar duplicados"""
    with conectar_sql() as conn:
        query = f"""
        SELECT InteractionIDKey, [Time], Usuario, Cola, Categoria, Codigo
        FROM {SQL_TABLE}
        WHERE [Time] BETWEEN ? AND ?
        """
        df = pd.read_sql(query, conn, params=[fecha_inicio, fecha_fin])
        return df

def filtrar_duplicados(df_nuevo, df_existente):
    """Filtra registros duplicados basándose en campos clave"""
    if df_existente.empty:
        return df_nuevo
    
    # Crear clave compuesta para comparación
    df_nuevo['clave_compuesta'] = (
        df_nuevo['InteractionIDKey'].astype(str) + '|' +
        df_nuevo['Time'].astype(str) + '|' +
        df_nuevo['Usuario'].astype(str) + '|' +
        df_nuevo['Cola'].astype(str) + '|' +
        df_nuevo['Categoria'].astype(str) + '|' +
        df_nuevo['Codigo'].astype(str)
    )
    
    df_existente['clave_compuesta'] = (
        df_existente['InteractionIDKey'].astype(str) + '|' +
        df_existente['Time'].astype(str) + '|' +
        df_existente['Usuario'].astype(str) + '|' +
        df_existente['Cola'].astype(str) + '|' +
        df_existente['Categoria'].astype(str) + '|' +
        df_existente['Codigo'].astype(str)
    )
    
    # Filtrar duplicados
    df_filtrado = df_nuevo[~df_nuevo['clave_compuesta'].isin(df_existente['clave_compuesta'])]
    
    # Eliminar columna temporal
    df_filtrado = df_filtrado.drop('clave_compuesta', axis=1)
    
    return df_filtrado

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

def procesar_chunk_10min(ses, fecha, hora_inicio, df_existente, reintentos=2):
    """Procesa un chunk de 10 minutos con filtrado de duplicados"""
    hora_fin = hora_inicio + 1/6  # 10 minutos
    
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
            df_nuevo = procesar(consultar_api(ses, start, end))
            tiempo_api = (datetime.now() - inicio).total_seconds()
            
            if not df_nuevo.empty:
                # Filtrar duplicados
                df_filtrado = filtrar_duplicados(df_nuevo, df_existente)
                
                if not df_filtrado.empty:
                    inicio_insert = datetime.now()
                    with conectar_sql() as cn:
                        nuevos = insertar_batch(df_filtrado, cn)
                    tiempo_insert = (datetime.now() - inicio_insert).total_seconds()
                    print(f"{datetime.now()} – ✅ {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}:{minuto_inicio:02d}h: {len(df_nuevo)} obtenidos → {len(df_filtrado)} únicos → {nuevos} insertados (API: {tiempo_api:.1f}s, SQL: {tiempo_insert:.1f}s)")
                    return len(df_filtrado)
                else:
                    print(f"{datetime.now()} – ℹ️ {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}:{minuto_inicio:02d}h: {len(df_nuevo)} obtenidos → 0 únicos (todos duplicados)")
                    return 0
            else:
                print(f"{datetime.now()} – ℹ️ {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}:{minuto_inicio:02d}h: Sin datos")
                return 0
        except Exception as e:
            if "400" in str(e) and intento < reintentos - 1:
                print(f"{datetime.now()} – ℹ️ {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}:{minuto_inicio:02d}h: Sin datos (API rechazó consulta)")
                return 0
            elif intento < reintentos - 1:
                print(f"{datetime.now()} – ⚠️ Reintento {intento+1}/{reintentos} para {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}:{minuto_inicio:02d}h: {e}")
                time.sleep(2)
            else:
                print(f"{datetime.now()} – ❌ Error final en {fecha.strftime('%Y-%m-%d')} {hora_inicio_int:02d}:{minuto_inicio:02d}h después de {reintentos} intentos: {e}")
                return 0

def procesar_analisis_ultima_hora(ses, fecha_inicio, fecha_fin):
    """Procesa el análisis desde 1 hora antes del último registro hasta la actualidad"""
    print(f"{datetime.now()} – 📅 Analizando desde {fecha_inicio.strftime('%Y-%m-%d %H:%M')} → {fecha_fin.strftime('%Y-%m-%d %H:%M')}")
    
    # Obtener registros existentes en el rango
    print(f"{datetime.now()} – 🔍 Obteniendo registros existentes para filtrado...")
    df_existente = obtener_registros_existentes(fecha_inicio, fecha_fin)
    print(f"{datetime.now()} – 📊 Registros existentes encontrados: {len(df_existente)}")
    
    # Crear chunks de 10 minutos
    chunks = []
    fecha_actual = fecha_inicio
    
    while fecha_actual < fecha_fin:
        hora = fecha_actual.hour + fecha_actual.minute / 60
        chunks.append((fecha_actual, hora))
        fecha_actual += timedelta(minutes=10)
    
    total_filas = 0
    total_obtenidas = 0
    chunks_exitosos = 0
    chunks_fallidos = 0
    
    # Procesar en paralelo con máximo 6 threads
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(procesar_chunk_10min, ses, fecha, hora, df_existente) for fecha, hora in chunks]
        
        for future in as_completed(futures):
            resultado = future.result()
            if resultado > 0:
                chunks_exitosos += 1
                total_filas += resultado
            else:
                chunks_fallidos += 1
    
    print(f"{datetime.now()} – 🎯 Análisis completado: TOTAL {total_filas} filas únicas insertadas (✅ {chunks_exitosos} chunks, ❌ {chunks_fallidos} chunks)")
    return total_filas

def ejecutar_analisis():
    """Ejecuta el análisis de la última hora"""
    print(f"{datetime.now()} – 🚀 Iniciando análisis de última hora")
    
    try:
        # 1. Obtener último registro
        ultimo_registro = obtener_ultimo_registro()
        print(f"{datetime.now()} – 📊 Último registro encontrado: {ultimo_registro}")
        
        # 2. Calcular ventana de análisis: desde 1 hora antes del último registro hasta ahora
        fecha_inicio = ultimo_registro - timedelta(hours=1)
        fecha_fin = datetime.now()
        
        print(f"{datetime.now()} – ⏰ Ventana de análisis: {fecha_inicio.strftime('%Y-%m-%d %H:%M')} → {fecha_fin.strftime('%Y-%m-%d %H:%M')}")
        
        # 3. Login
        ses = login()
        
        # 4. Procesar análisis
        filas_procesadas = procesar_analisis_ultima_hora(ses, fecha_inicio, fecha_fin)
        
        print(f"{datetime.now()} – 🎉 Análisis completado! {filas_procesadas} filas únicas procesadas")
        
    except Exception as e:
        print(f"{datetime.now()} – ❌ Error en análisis: {e}")

def main():
    """Ejecuta el análisis una sola vez"""
    print(f"{datetime.now()} – 🕐 Iniciando análisis de última hora")
    
    try:
        ejecutar_analisis()
        print(f"{datetime.now()} – ✅ Análisis finalizado exitosamente")
    except KeyboardInterrupt:
        print(f"{datetime.now()} – ⏹️ Análisis interrumpido por el usuario")
    except Exception as e:
        print(f"{datetime.now()} – ❌ Error en el análisis: {e}")

if __name__ == "__main__":
    main() 