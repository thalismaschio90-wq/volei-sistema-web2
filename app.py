from flask import Flask
import os

from extensions import socketio

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

from banco import (
    criar_tabela_atletas,
    criar_tabelas_grupos,
    criar_tabela_partidas,
    criar_tabelas_oficiais,
    criar_campos_regras_operacionais_competicoes,
    criar_campos_travamento_competicoes,
    criar_tabela_solicitacoes_treinador,
)

app = Flask(__name__)
app.secret_key = "voleitablepro"

socketio.init_app(app)

criar_tabela_atletas()
criar_tabelas_grupos()
criar_tabela_partidas()
criar_tabelas_oficiais()
criar_campos_regras_operacionais_competicoes()
criar_campos_travamento_competicoes()
criar_tabela_solicitacoes_treinador()

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

import socket_events  # noqa: E402,F401

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug_mode = os.environ.get("FLASK_DEBUG", "False").lower() == "true"
    socketio.run(app, host="0.0.0.0", port=port, debug=debug_mode)