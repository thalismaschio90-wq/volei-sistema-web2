from flask import (
    Flask,
    redirect,
    send_from_directory,
    make_response,
    render_template,
)
import os

from extensions import socketio

# ============================================================
# 🔥 BANCO / ESTRUTURAS
# ============================================================
from banco import (
    criar_estrutura_rotacao_profissional,
    criar_tabela_atalhos_apontador,
)

# ============================================================
# 🔥 BLUEPRINTS
# ============================================================
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
from routes.demo import demo_bp
from routes.jogo_avulso import jogo_avulso_bp

# ============================================================
# 🔥 APP TEMPO REAL
# ============================================================
try:
    from routes.app_tempo_real import app_tempo_real_bp
except Exception:
    app_tempo_real_bp = None


# ============================================================
# 🔥 APP
# ============================================================
app = Flask(
    __name__,
    static_folder="static",
    template_folder="templates",
)

app.secret_key = os.environ.get(
    "SECRET_KEY",
    "voleitablepro"
)

# ============================================================
# 🔥 BANCO
# ============================================================
try:
    criar_estrutura_rotacao_profissional()
except Exception as e:
    print("ERRO estrutura rotação:", e)

try:
    criar_tabela_atalhos_apontador()
except Exception as e:
    print("ERRO tabela atalhos:", e)

# ============================================================
# 🔥 SOCKET IO
# ============================================================
socketio.init_app(
    app,
    cors_allowed_origins="*",
    ping_timeout=20,
    ping_interval=10,
)

# ============================================================
# 🔥 BLUEPRINTS
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
app.register_blueprint(demo_bp)
app.register_blueprint(jogo_avulso_bp)

if app_tempo_real_bp is not None:
    app.register_blueprint(app_tempo_real_bp)
    print("✅ app_tempo_real carregado")
else:
    print("⚠️ app_tempo_real não encontrado")

# ============================================================
# 🔥 LANDING PAGE
# ============================================================
@app.route("/")
def home():
    return render_template("landing.html")


# ============================================================
# 🔥 REDIRECIONAR /inicio
# ============================================================
@app.route("/inicio")
def inicio_publico():
    return render_template("landing.html")


# ============================================================
# 🔥 HEALTH CHECK
# ============================================================
@app.route("/healthz")
def healthz():
    return "ok", 200


# ============================================================
# 🔥 MANIFEST
# ============================================================
@app.route("/manifest.json")
def manifest_pwa():

    resposta = make_response(
        send_from_directory(
            "static",
            "manifest.json"
        )
    )

    resposta.headers["Content-Type"] = "application/manifest+json"
    resposta.headers["Cache-Control"] = "no-cache"

    return resposta


# ============================================================
# 🔥 SERVICE WORKER
# ============================================================
@app.route("/sw.js")
def service_worker_pwa():

    resposta = make_response(
        send_from_directory(
            "static",
            "sw.js"
        )
    )

    resposta.headers["Content-Type"] = "application/javascript"
    resposta.headers["Cache-Control"] = "no-cache"

    return resposta


# ============================================================
# 🔥 SOCKET EVENTS
# ============================================================
import socket_events  # noqa


# ============================================================
# 🔥 EXECUÇÃO
# ============================================================
if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            5000
        )
    )

    debug_mode = (
        os.environ.get(
            "FLASK_DEBUG",
            "False"
        ).lower() == "true"
    )

    socketio.run(
        app,
        host="0.0.0.0",
        port=port,
        debug=debug_mode,
        allow_unsafe_werkzeug=True,
    )