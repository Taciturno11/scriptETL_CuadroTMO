#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de prueba para verificar que todas las dependencias estén instaladas
"""

import sys
import traceback

def test_imports():
    """Prueba todas las importaciones necesarias"""
    print("🔍 Probando importaciones...")
    
    try:
        print("  - Importando requests...")
        import requests
        print("    ✅ requests importado correctamente")
    except Exception as e:
        print(f"    ❌ Error importando requests: {e}")
        return False
    
    try:
        print("  - Importando pandas...")
        import pandas as pd
        print("    ✅ pandas importado correctamente")
    except Exception as e:
        print(f"    ❌ Error importando pandas: {e}")
        return False
    
    try:
        print("  - Importando pyodbc...")
        import pyodbc
        print("    ✅ pyodbc importado correctamente")
    except Exception as e:
        print(f"    ❌ Error importando pyodbc: {e}")
        return False
    
    try:
        print("  - Importando pytz...")
        import pytz
        print("    ✅ pytz importado correctamente")
    except Exception as e:
        print(f"    ❌ Error importando pytz: {e}")
        return False
    
    try:
        print("  - Importando datetime...")
        from datetime import datetime, timedelta
        print("    ✅ datetime importado correctamente")
    except Exception as e:
        print(f"    ❌ Error importando datetime: {e}")
        return False
    
    print("🎉 Todas las importaciones fueron exitosas!")
    return True

if __name__ == "__main__":
    print("🚀 Iniciando prueba de importaciones...")
    success = test_imports()
    
    if success:
        print("\n✅ El script puede ejecutarse correctamente")
        sys.exit(0)
    else:
        print("\n❌ Hay problemas con las importaciones")
        sys.exit(1)
