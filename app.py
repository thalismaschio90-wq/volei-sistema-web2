from flask import (
    Flask,
    request,
    session,
    redirect,
    send_from_directory,
    make_response,
    render_template,
)
import os

from extensions import socketio

from banco import (
    criar_estrutura_rotacao_profissional,
    criar_tabela_atalhos_apontador,
    criar_tabela_equipes_competicoes,
    criar_campos_perfil_equipe,
    criar_campo_escudo_equipes,
    garantir_campos_trava_operacional_partida,
    criar_tabela_destaques_partida,
    buscar_equipe_por_login,
    escudo_padrao_equipe,
    migrar_escudos_arquivos_para_banco,
    listar_escudos_status,
)

from routes.auth import auth_bp
from routes.acessos_pin import acessos_pin_bp
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
from routes.offline_config import offline_config_bp
from routes.bootstrap import bootstrap_bp

try:
    from routes.app_tempo_real import app_tempo_real_bp
except Exception:
    app_tempo_real_bp = None


app = Flask(
    __name__,
    static_folder="static",
    template_folder="templates",
)

app.secret_key = os.environ.get(
    "SECRET_KEY",
    "voleitablepro"
)

# 🔥 Evita cache quebrado em iPhone/Safari/PWA
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 60 * 60 * 24 * 30


# Chaves que nunca devem ficar na cookie-session do Flask.
# O Flask assina a session no próprio cookie do navegador; guardar dicts grandes
# ou escudos base64 aqui estoura o limite de ~4KB e quebra login no Safari/mobile.
_SESSION_CHAVES_PESADAS = {
    "topbar_equipe_cache",
    "partidas",
    "equipes",
    "atletas",
    "classificacao",
    "estado",
    "competicao_obj",
    "dados",
    "eventos",
    "papeleta",
    "escudo",
    "escudo_blob",
}


def _limpar_session_pesada():
    removidas = False
    for chave in list(_SESSION_CHAVES_PESADAS):
        if chave in session:
            session.pop(chave, None)
            removidas = True

    # Compatibilidade: remove qualquer cache/dado grande salvo por versões anteriores.
    for chave in list(session.keys()):
        valor = session.get(chave)
        if chave.endswith("_cache") and chave not in {"_flashes"}:
            session.pop(chave, None)
            removidas = True
            continue
        if isinstance(valor, (dict, list, tuple)) and chave not in {"_flashes"}:
            texto = str(valor)
            if len(texto) > 1200:
                session.pop(chave, None)
                removidas = True
        elif isinstance(valor, str) and len(valor) > 1800:
            session.pop(chave, None)
            removidas = True
    return removidas


@app.before_request
def limpar_session_pesada_antes_request():
    _limpar_session_pesada()


@app.after_request
def aplicar_headers_cache(response):
    path = request.path or ""

    # Performance: estáticos versionados podem ficar no navegador.
    # Páginas dinâmicas e arquivos críticos do PWA continuam sem cache agressivo.
    if path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=2592000"
        response.headers.pop("Pragma", None)
        response.headers.pop("Expires", None)
    elif path in ("/sw.js", "/manifest.json", "/manifest-arbitro.json", "/app-login", "/app", "/arbitro"):
        response.headers["Cache-Control"] = "no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"

    return response


try:
    criar_estrutura_rotacao_profissional()
except Exception as e:
    print("ERRO estrutura rotação:", e)

try:
    criar_tabela_atalhos_apontador()
except Exception as e:
    print("ERRO tabela atalhos:", e)

try:
    criar_tabela_equipes_competicoes()
except Exception as e:
    print("ERRO tabela equipes_competicoes:", e)

try:
    criar_campos_perfil_equipe()
except Exception as e:
    print("ERRO campos perfil equipe:", e)

try:
    criar_campo_escudo_equipes()
except Exception as e:
    print("ERRO campo escudo equipe:", e)

try:
    garantir_campos_trava_operacional_partida()
except Exception as e:
    print("ERRO campos trava operacional:", e)

try:
    # DDL da finalização é preparado somente na inicialização/deploy.
    # Nunca executar CREATE/ALTER/INDEX durante o clique do apontador.
    criar_tabela_destaques_partida()
except Exception as e:
    print("ERRO tabela destaques_partida:", e)

try:
    os.makedirs(os.path.join(app.static_folder, "uploads", "escudos"), exist_ok=True)
    os.makedirs(os.path.join(app.static_folder, "img"), exist_ok=True)
except Exception as e:
    print("ERRO pastas upload escudos:", e)


socketio.init_app(
    app,
    cors_allowed_origins="*",
    ping_timeout=20,
    ping_interval=10,
)


@app.context_processor
def injetar_dados_topbar_global():
    """
    Deixa o cabeçalho global com os dados corretos em qualquer template.
    Principal uso: mostrar o escudo da equipe logada ao lado do nome no topo.
    """
    dados_padrao = {
        "topbar_nome": session.get("nome") or session.get("usuario") or "",
        "topbar_perfil": session.get("perfil") or "",
        "topbar_escudo": "",
        "topbar_tem_escudo": False,
        "topbar_url": "/minha-conta",
    }

    if not session.get("usuario"):
        return dados_padrao

    perfil = (session.get("perfil") or "").strip().lower()
    usuario = (session.get("usuario") or "").strip()

    try:
        escudo_padrao = escudo_padrao_equipe() or "/static/img/escudo_padrao.svg"
    except Exception:
        escudo_padrao = "/static/img/escudo_padrao.svg"

    dados_padrao["topbar_escudo"] = escudo_padrao

    if perfil == "equipe" and usuario:
        equipe = None
        competicao_atual = (session.get("competicao_equipe_atual") or "").strip() or None

        # Nunca cacheia a equipe dentro da session: escudo em base64 pode passar de
        # 30KB e quebrar o cookie no iPhone/Safari. Busca de forma leve a cada render.
        try:
            equipe = buscar_equipe_por_login(usuario, competicao_atual)
        except Exception as e:
            print("AVISO topbar equipe por competição:", e)
            equipe = None

        if not equipe:
            try:
                equipe = buscar_equipe_por_login(usuario, None)
            except Exception as e:
                print("AVISO topbar equipe global:", e)
                equipe = None

        if equipe:
            nome_equipe = (
                equipe.get("nome")
                or equipe.get("nome_equipe")
                or session.get("nome")
                or usuario
            )
            escudo_equipe = (
                equipe.get("escudo_exibicao")
                or equipe.get("escudo_blob")
                or equipe.get("escudo")
                or equipe.get("escudo_url")
                or equipe.get("logo")
                or equipe.get("logo_url")
                or escudo_padrao
            )

            dados_padrao.update({
                "topbar_nome": nome_equipe,
                "topbar_perfil": "EQUIPE",
                "topbar_escudo": escudo_equipe,
                "topbar_tem_escudo": True,
                "topbar_url": "/minha-equipe",
            })
        else:
            dados_padrao.update({
                "topbar_perfil": "EQUIPE",
                "topbar_tem_escudo": True,
                "topbar_url": "/minha-equipe",
            })

    return dados_padrao


app.register_blueprint(acessos_pin_bp)
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
app.register_blueprint(offline_config_bp)
app.register_blueprint(bootstrap_bp)

if app_tempo_real_bp is not None:
    app.register_blueprint(app_tempo_real_bp)
    print("✅ app_tempo_real carregado")
else:
    print("⚠️ app_tempo_real não encontrado")


def eh_mobile_ou_app():
    ua = (request.headers.get("User-Agent") or "").lower()

    return (
        request.args.get("app") == "1"
        or "electron" in ua
        or "iphone" in ua
        or "ipad" in ua
        or "android" in ua
        or "mobile" in ua
    )


@app.route("/")
def home():
    if eh_mobile_ou_app():
        return redirect("/app-login?app=1&v=20260528-offline1")

    return render_template("landing.html")


@app.route("/inicio")
def inicio_publico():
    if eh_mobile_ou_app():
        return redirect("/app-login?app=1&v=20260528-offline1")

    return render_template("landing.html")


@app.route("/app")
@app.route("/app-login")
def app_login_pwa():
    resposta = make_response(
        render_template("app_login.html")
    )

    resposta.headers["Cache-Control"] = "no-cache, must-revalidate, max-age=0"
    resposta.headers["Pragma"] = "no-cache"
    resposta.headers["Expires"] = "0"

    return resposta




@app.route("/admin/migrar-escudos-neon")
def admin_migrar_escudos_neon():
    if (session.get("perfil") or "").strip().lower() != "superadmin":
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403

    resultado = migrar_escudos_arquivos_para_banco(app.static_folder)
    return {"ok": True, "resultado": resultado}


@app.route("/admin/status-escudos")
def admin_status_escudos():
    if (session.get("perfil") or "").strip().lower() != "superadmin":
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403

    return {"ok": True, "equipes": listar_escudos_status()}


@app.route("/healthz")
def healthz():
    return "ok", 200


@app.route("/manifest.json")
def manifest_pwa():
    resposta = make_response(
        send_from_directory(
            "static",
            "manifest.json"
        )
    )

    resposta.headers["Content-Type"] = "application/manifest+json"
    resposta.headers["Cache-Control"] = "no-cache, must-revalidate, max-age=0"
    resposta.headers["Pragma"] = "no-cache"
    resposta.headers["Expires"] = "0"

    return resposta


@app.route("/manifest-arbitro.json")
def manifest_arbitro_pwa():
    resposta = make_response(
        send_from_directory(
            "static",
            "manifest-arbitro.json"
        )
    )

    resposta.headers["Content-Type"] = "application/manifest+json"
    resposta.headers["Cache-Control"] = "no-cache, must-revalidate, max-age=0"
    resposta.headers["Pragma"] = "no-cache"
    resposta.headers["Expires"] = "0"

    return resposta


@app.route("/sw.js")
def service_worker_pwa():
    resposta = make_response(
        send_from_directory(
            "static",
            "sw.js"
        )
    )

    resposta.headers["Content-Type"] = "application/javascript"
    resposta.headers["Service-Worker-Allowed"] = "/"
    resposta.headers["Cache-Control"] = "no-cache, must-revalidate, max-age=0"
    resposta.headers["Pragma"] = "no-cache"
    resposta.headers["Expires"] = "0"

    return resposta


import socket_events  # noqa


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