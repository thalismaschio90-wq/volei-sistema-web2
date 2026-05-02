from flask import Flask, redirect
import os

from extensions import socketio

# 🔥 IMPORTANTE (ROTAÇÃO PROFISSIONAL)
from banco import criar_estrutura_rotacao_profissional

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


app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "voleitablepro")

# 🔥 ESSENCIAL — GARANTE BANCO PRONTO
criar_estrutura_rotacao_profissional()

socketio.init_app(
    app,
    cors_allowed_origins="*",
)

# 🔥 REGISTRO DAS ROTAS
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


@app.route("/")
def home():
    return redirect("/login")


@app.route("/healthz")
def healthz():
    return "ok", 200


# 🔥 SOCKET EVENTS (DEIXA NO FINAL)
import socket_events  # noqa: E402,F401


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