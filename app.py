from flask import Flask, redirect, send_from_directory, make_response
import os

from extensions import socketio

# 🔥 BANCO / ESTRUTURAS
from banco import (
    criar_estrutura_rotacao_profissional,
    criar_tabela_atalhos_apontador,
)

# 🔥 BLUEPRINTS PRINCIPAIS
from routes.auth import auth_bp
from routes.painel import painel_bp
from routes.competicoes import competicoes_bp
from routes.equipes import equipes_bp
from routes.arbitros import arbitros_bp
from routes.tabela import tabela_bp
from routes.minha_conta import minha_conta_bp
from routes.oficiais import oficiais_bp
from routes.apontadores import apontadores_bp
from routes.formato_competicao import formato_competicao_bp
from routes.treinador import treinador_bp
from routes.relatorios import relatorios_bp

# 🔥 NOVA TELA ULTRA LEVE / APP TEMPO REAL
try:
    from routes.app_tempo_real import app_tempo_real_bp
except Exception:
    app_tempo_real_bp = None


app = Flask(
    __name__,
    static_folder="static",
    template_folder="templates",
)

app.secret_key = os.environ.get("SECRET_KEY", "voleitablepro")


# ============================================================
# 🔥 GARANTIR ESTRUTURAS DO BANCO
# ============================================================
try:
    criar_estrutura_rotacao_profissional()
except Exception as e:
    print("⚠️ Erro ao criar estrutura de rotação profissional:", e)

try:
    criar_tabela_atalhos_apontador()
except Exception as e:
    print("⚠️ Erro ao criar tabela de atalhos do apontador:", e)


# ============================================================
# 🔥 SOCKET.IO
# ============================================================
socketio.init_app(
    app,
    cors_allowed_origins="*",
    ping_timeout=20,
    ping_interval=10,
)


# ============================================================
# 🔥 REGISTRO DAS ROTAS
# ============================================================
app.register_blueprint(auth_bp)
app.register_blueprint(painel_bp)
app.register_blueprint(competicoes_bp)
app.register_blueprint(equipes_bp)
app.register_blueprint(arbitros_bp)
app.register_blueprint(tabela_bp)
app.register_blueprint(minha_conta_bp)
app.register_blueprint(oficiais_bp)
app.register_blueprint(apontadores_bp)
app.register_blueprint(formato_competicao_bp)
app.register_blueprint(treinador_bp)
app.register_blueprint(relatorios_bp)

# 🔥 REGISTRA A NOVA ROTA DO APP TEMPO REAL SE EXISTIR
if app_tempo_real_bp is not None:
    app.register_blueprint(app_tempo_real_bp)
    print("✅ Blueprint app_tempo_real registrado com sucesso.")
else:
    print("⚠️ Blueprint app_tempo_real ainda não encontrado. Sistema continua normal.")


# ============================================================
# 🔥 ROTAS BÁSICAS
# ============================================================
@app.route("/")
def home():
    return redirect("/login")


@app.route("/healthz")
def healthz():
    return "ok", 200


@app.route("/manifest.json")
def manifest_pwa():
    resposta = make_response(send_from_directory("static", "manifest.json"))
    resposta.headers["Content-Type"] = "application/manifest+json"
    resposta.headers["Cache-Control"] = "no-cache"
    return resposta


@app.route("/sw.js")
def service_worker_pwa():
    resposta = make_response(send_from_directory("static", "sw.js"))
    resposta.headers["Content-Type"] = "application/javascript"
    resposta.headers["Cache-Control"] = "no-cache"
    return resposta


# ============================================================
# 🔥 SOCKET EVENTS
# DEIXAR SEMPRE NO FINAL, DEPOIS DE REGISTRAR APP E ROTAS
# ============================================================
import socket_events  # noqa: E402,F401


# ============================================================
# 🔥 EXECUÇÃO LOCAL
# ============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug_mode = os.environ.get("FLASK_DEBUG", "False").lower() == "true"

    socketio.run(
        app,
        host="0.0.0.0",
        port=port,
        debug=debug_mode,
        allow_unsafe_werkzeug=True,
    )