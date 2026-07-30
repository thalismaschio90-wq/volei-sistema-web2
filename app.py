from flask import (
    Flask,
    request,
    session,
    redirect,
    send_from_directory,
    make_response,
    render_template,
    jsonify,
    Response,
)
import os

from extensions import socketio
from core.performance import registrar_instrumentacao_performance
from core.performance_store import performance_store
from realtime.delta_metrics import delta_metrics_store
from realtime.event_priority import dispatch_metrics_store
from realtime.load_shedding import load_shedding_manager
from core.performance_export import exportar_json, exportar_markdown
from core.readiness import readiness_report
from core.runtime_config import load_runtime_config
from core.audit_context import (
    definir_contexto_auditoria,
    limpar_contexto_auditoria,
    montar_contexto_auditoria,
)
from repositories.conexao import obter_estatisticas_pool
from core.security import carregar_secret_key
from core.csrf import registrar_csrf

from banco import (
    escudo_padrao_equipe,
    migrar_escudos_arquivos_para_banco,
    listar_escudos_status,
)
from services.equipes.consultas import buscar_equipe_por_login
from services.ui.topbar import buscar_equipe_topbar

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
from routes.replay import replay_bp
from routes.scout_inteligente import scout_inteligente_bp
from routes.impacto_competitivo import impacto_competitivo_bp
from routes.dashboard_operacional import dashboard_operacional_bp

try:
    from routes.app_tempo_real import app_tempo_real_bp
except Exception:
    app_tempo_real_bp = None


app = Flask(
    __name__,
    static_folder="static",
    template_folder="templates",
)

app.secret_key = carregar_secret_key()

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE=os.environ.get("SESSION_COOKIE_SAMESITE", "Lax"),
    SESSION_COOKIE_SECURE=str(os.environ.get("SESSION_COOKIE_SECURE", "1" if os.environ.get("RENDER") else "0")).strip().lower() in {"1", "true", "yes", "on"},
    PERMANENT_SESSION_LIFETIME=int(os.environ.get("SESSION_LIFETIME_SECONDS", "43200")),
    MAX_CONTENT_LENGTH=int(os.environ.get("MAX_UPLOAD_BYTES", str(8 * 1024 * 1024))),
)

registrar_csrf(app)

# 🔥 Evita cache quebrado em iPhone/Safari/PWA
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 60 * 60 * 24 * 30

# Instrumentação opcional, sem impacto quando desabilitada.
registrar_instrumentacao_performance(app)


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
def preparar_request():
    _limpar_session_pesada()
    contexto = montar_contexto_auditoria(
        usuario=session.get("usuario"),
        nome=session.get("nome"),
        perfil=session.get("perfil"),
        endpoint=request.endpoint,
        metodo=request.method,
        caminho=request.path,
        ip=request.headers.get("X-Forwarded-For", request.remote_addr),
        user_agent=request.headers.get("User-Agent"),
        request_id=request.headers.get("X-Request-ID"),
    )
    request._vtp_audit_token = definir_contexto_auditoria(contexto)


@app.teardown_request
def limpar_auditoria_request(_erro=None):
    token = getattr(request, "_vtp_audit_token", None)
    limpar_contexto_auditoria(token)


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



# Alterações de schema são executadas uma única vez pelo comando de migração
# antes do Gunicorn iniciar. Nenhum worker executa DDL ao importar ``app.py``.

try:
    os.makedirs(os.path.join(app.static_folder, "uploads", "escudos"), exist_ok=True)
    os.makedirs(os.path.join(app.static_folder, "img"), exist_ok=True)
except Exception as e:
    print("ERRO pastas upload escudos:", e)


socketio.init_app(app)


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

        # Nunca salva escudo/base64 na session. O cache fica somente no servidor
        # e evita consultar o PostgreSQL em toda renderização de template.
        try:
            equipe = buscar_equipe_topbar(
                usuario,
                competicao_atual,
                buscar_equipe_por_login,
            )
        except Exception as e:
            print("AVISO topbar equipe:", e)
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
app.register_blueprint(replay_bp)
app.register_blueprint(scout_inteligente_bp)
app.register_blueprint(impacto_competitivo_bp)
app.register_blueprint(dashboard_operacional_bp)

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
    # Liveness: confirma apenas que o processo Flask está respondendo.
    return "ok", 200


@app.route("/readyz")
def readyz():
    # Readiness: verifica PostgreSQL, estado vivo e segurança da configuração.
    relatorio = readiness_report(
        ttl_seconds=float(os.environ.get("READINESS_CACHE_SECONDS", "5") or 5)
    )
    status = 200 if relatorio.get("ok") else 503
    return jsonify(relatorio), status


@app.route("/admin/runtime-status")
def admin_runtime_status():
    if (session.get("perfil") or "").strip().lower() != "superadmin":
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403

    relatorio = readiness_report(ttl_seconds=1, force=True)
    relatorio["pool"] = obter_estatisticas_pool()
    relatorio["runtime_config"] = load_runtime_config().public_dict()
    return jsonify(relatorio), (200 if relatorio.get("ok") else 503)


@app.route("/admin/performance")
def admin_performance():
    if (session.get("perfil") or "").strip().lower() != "superadmin":
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    return render_template("admin_performance.html", dados=performance_store.snapshot())


@app.route("/admin/performance-status")
def admin_performance_status():
    if (session.get("perfil") or "").strip().lower() != "superadmin":
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    return jsonify(performance_store.snapshot())


@app.route("/admin/performance/exportar.json")
def admin_performance_exportar_json():
    if (session.get("perfil") or "").strip().lower() != "superadmin":
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    conteudo = exportar_json(performance_store.snapshot())
    return Response(
        conteudo,
        mimetype="application/json; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=volleytablepro-performance.json"},
    )


@app.route("/admin/performance/exportar.md")
def admin_performance_exportar_markdown():
    if (session.get("perfil") or "").strip().lower() != "superadmin":
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    conteudo = exportar_markdown(performance_store.snapshot())
    return Response(
        conteudo,
        mimetype="text/markdown; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=volleytablepro-performance.md"},
    )


@app.post("/admin/performance/limpar")
def admin_performance_limpar():
    if (session.get("perfil") or "").strip().lower() != "superadmin":
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    performance_store.limpar()
    return redirect("/admin/performance")


@app.post("/realtime/delta-telemetria")
def realtime_delta_telemetria():
    if not delta_metrics_store.permitir_origem(request.remote_addr):
        return {"ok": False, "erro": "limite"}, 429
    dados = request.get_json(silent=True) or {}
    tipo_cliente = dados.get("tipo_cliente")
    eventos = dados.get("eventos")
    aceitos = 0
    if isinstance(eventos, list):
        for item in eventos[:20]:
            if not isinstance(item, dict):
                continue
            if delta_metrics_store.registrar_cliente(
                tipo_cliente, item.get("evento"), item.get("quantidade", 1)
            ):
                aceitos += 1
    elif delta_metrics_store.registrar_cliente(
        tipo_cliente, dados.get("evento"), dados.get("quantidade", 1)
    ):
        aceitos = 1

    renderizacoes_aceitas = 0
    renderizacoes = dados.get("renderizacoes")
    if isinstance(renderizacoes, list):
        for item in renderizacoes[:20]:
            if not isinstance(item, dict):
                continue
            if delta_metrics_store.registrar_render_cliente(
                tipo_cliente,
                item.get("duracao_ms"),
                item.get("quantidade_agregada", 1),
            ):
                renderizacoes_aceitas += 1

    total_aceitos = aceitos + renderizacoes_aceitas
    return {
        "ok": total_aceitos > 0,
        "eventos_aceitos": aceitos,
        "renderizacoes_aceitas": renderizacoes_aceitas,
    }, (200 if total_aceitos else 400)




def _snapshot_realtime_completo():
    dados = delta_metrics_store.snapshot()
    dados["despacho"] = dispatch_metrics_store.snapshot()
    dados["degradacao"] = load_shedding_manager.snapshot()
    return dados

@app.route("/admin/realtime-delta")
def admin_realtime_delta():
    if (session.get("perfil") or "").strip().lower() != "superadmin":
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    return render_template("admin_realtime_delta.html", dados=_snapshot_realtime_completo())


@app.route("/admin/realtime-delta-status")
def admin_realtime_delta_status():
    if (session.get("perfil") or "").strip().lower() != "superadmin":
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    return jsonify(_snapshot_realtime_completo())


@app.post("/admin/realtime-delta/limpar")
def admin_realtime_delta_limpar():
    if (session.get("perfil") or "").strip().lower() != "superadmin":
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    delta_metrics_store.limpar()
    dispatch_metrics_store.limpar()
    load_shedding_manager.limpar()
    return redirect("/admin/realtime-delta")


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