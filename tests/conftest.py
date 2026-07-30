import sys
import types
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

# O ambiente de validação não possui psycopg. Estes stubs permitem testar
# regras puras sem abrir banco; em produção o pacote real vem do requirements.
if "psycopg" not in sys.modules:
    psycopg = types.ModuleType("psycopg")
    psycopg.connect = lambda *args, **kwargs: None
    rows = types.ModuleType("psycopg.rows")
    rows.dict_row = object()
    sys.modules["psycopg"] = psycopg
    sys.modules["psycopg.rows"] = rows
if "psycopg_pool" not in sys.modules:
    pool = types.ModuleType("psycopg_pool")
    pool.ConnectionPool = object
    sys.modules["psycopg_pool"] = pool
