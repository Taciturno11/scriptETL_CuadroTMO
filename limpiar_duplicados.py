# limpiar_duplicados.py – Limpia duplicados existentes
import pyodbc
from datetime import datetime

# ───────── Configuración ─────────
SQL_SERVER   = "172.16.248.48"
SQL_DATABASE = "Partner"
SQL_USER     = "anubis"
SQL_PASSWORD = "Tg7#kPz9@rLt2025"
SQL_TABLE    = "dbo.TransferenciasClaro"

def conectar_sql():
    return pyodbc.connect(
        "DRIVER={SQL Server};"
        f"SERVER={SQL_SERVER},1433;DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASSWORD};TrustServerCertificate=yes;"
    )

def contar_registros():
    """Cuenta el total de registros en la tabla"""
    with conectar_sql() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {SQL_TABLE}")
        return cur.fetchone()[0]

def contar_duplicados():
    """Cuenta cuántos duplicados hay"""
    with conectar_sql() as conn:
        cur = conn.cursor()
        query = f"""
        SELECT COUNT(*) - COUNT(DISTINCT InteractionIDKey) as duplicados
        FROM {SQL_TABLE}
        """
        cur.execute(query)
        return cur.fetchone()[0]

def eliminar_duplicados():
    """Elimina registros duplicados manteniendo solo una fila por InteractionIDKey"""
    with conectar_sql() as conn:
        cur = conn.cursor()
        print(f"{datetime.now()} – 🧹 Eliminando duplicados...")
        
        # Eliminar duplicados manteniendo solo una fila por InteractionIDKey
        delete_query = f"""
        WITH DuplicatesCTE AS (
            SELECT 
                InteractionIDKey,
                [Time],
                Usuario,
                Cola,
                Categoria,
                Codigo,
                ROW_NUMBER() OVER (
                    PARTITION BY InteractionIDKey, [Time], Usuario, Cola, Categoria, Codigo 
                    ORDER BY InteractionIDKey
                ) as rn
            FROM {SQL_TABLE}
        )
        DELETE FROM {SQL_TABLE}
        WHERE InteractionIDKey IN (
            SELECT InteractionIDKey 
            FROM DuplicatesCTE 
            WHERE rn > 1
        )
        """
        
        cur.execute(delete_query)
        filas_eliminadas = cur.rowcount
        conn.commit()
        print(f"{datetime.now()} – ✅ Duplicados eliminados: {filas_eliminadas} filas")
        return filas_eliminadas

def main():
    print(f"{datetime.now()} – 🚀 Iniciando limpieza de duplicados")
    
    # 1. Contar registros antes
    total_antes = contar_registros()
    duplicados_antes = contar_duplicados()
    
    print(f"{datetime.now()} – 📊 Estado actual:")
    print(f"   • Total registros: {total_antes}")
    print(f"   • Duplicados estimados: {duplicados_antes}")
    
    # 2. Eliminar duplicados
    filas_eliminadas = eliminar_duplicados()
    
    # 3. Contar registros después
    total_despues = contar_registros()
    
    print(f"{datetime.now()} – 📊 Estado final:")
    print(f"   • Total registros: {total_despues}")
    print(f"   • Registros eliminados: {filas_eliminadas}")
    print(f"   • Diferencia: {total_antes - total_despues}")
    
    print(f"{datetime.now()} – 🎉 Limpieza completada!")

if __name__ == "__main__":
    main() 