from flask import (
    Flask,
    request,
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
)

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
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0


@app.after_request
def aplicar_headers_cache(response):
    path = request.path or ""

    if (
        path.endswith(".css")
        or path.endswith(".js")
        or path.endswith(".json")
        or path == "/sw.js"
        or path == "/manifest.json"
        or path == "/app-login"
        or path == "/app"
    ):
        response.headers["Cache-Control"] = (
            "no-cache, no-store, must-revalidate, max-age=0"
        )
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


socketio.init_app(
    app,
    cors_allowed_origins="*",
    ping_timeout=20,
    ping_interval=10,
)


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

    resposta.headers["Cache-Control"] = (
        "no-cache, no-store, must-revalidate, max-age=0"
    )
    resposta.headers["Pragma"] = "no-cache"
    resposta.headers["Expires"] = "0"

    return resposta




@app.route("/offline-apontador")
def offline_apontador_pwa():
    """Tela local de entrada offline do apontador.

    Esta página é salva no cache do Service Worker. Quando o app abre sem internet
    e a sessão Flask/cookie não consegue ser validada, o SW entrega esta tela.
    Ela NÃO autentica no servidor: apenas libera o acesso às partidas que já foram
    baixadas neste navegador/dispositivo.
    """
    html = """
<!doctype html>
<html lang="pt-BR">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="theme-color" content="#0f2f4a">
    <title>VolleyTable Pro - Offline</title>
    <style>
        :root { --azul:#0f2f4a; --borda:#d6e2f0; --bg:#f3f7fb; --muted:#64748b; }
        * { box-sizing: border-box; }
        body { margin:0; min-height:100vh; font-family: Arial, sans-serif; background:linear-gradient(180deg,#eef6ff,#ffffff); color:#183247; display:flex; align-items:center; justify-content:center; padding:18px; }
        .card { width:min(760px,100%); background:#fff; border:1px solid var(--borda); border-radius:24px; box-shadow:0 20px 60px rgba(15,47,74,.16); padding:22px; }
        .marca { display:flex; align-items:center; gap:12px; margin-bottom:16px; }
        .logo { width:52px; height:52px; border-radius:16px; background:var(--azul); color:white; display:flex; align-items:center; justify-content:center; font-weight:1000; font-size:22px; }
        h1 { margin:0; font-size:24px; line-height:1.1; }
        .sub { color:var(--muted); font-weight:700; margin-top:4px; }
        .aviso { border:1px solid #fdba74; background:#fff7ed; color:#9a3412; padding:12px 14px; border-radius:16px; font-weight:800; margin:16px 0; }
        .lista { display:grid; gap:10px; margin-top:14px; }
        .partida { border:1px solid var(--borda); border-radius:18px; padding:14px; background:#f8fbff; display:grid; gap:8px; }
        .titulo { font-weight:1000; font-size:17px; }
        .meta { color:var(--muted); font-size:13px; font-weight:800; }
        .acoes { display:flex; gap:8px; flex-wrap:wrap; margin-top:6px; }
        button, a.btn { border:0; border-radius:12px; padding:11px 14px; font-weight:1000; cursor:pointer; text-decoration:none; display:inline-flex; align-items:center; justify-content:center; }
        .primario { background:var(--azul); color:white; }
        .sec { background:#e2e8f0; color:#0f172a; }
        .danger { background:#fee2e2; color:#991b1b; }
        .vazio { text-align:center; color:var(--muted); font-weight:800; padding:24px 8px; }
        .status { font-size:13px; color:var(--muted); font-weight:800; margin-top:14px; }
    </style>
</head>
<body>
    <main class="card">
        <div class="marca">
            <div class="logo">VT</div>
            <div>
                <h1>Modo offline do apontador</h1>
                <div class="sub">Acesso local às partidas baixadas neste dispositivo.</div>
            </div>
        </div>
        <div id="aviso" class="aviso">Você está sem internet ou sem sessão online. Só aparecem aqui as partidas preparadas antes.</div>
        <div id="lista" class="lista"></div>
        <div class="acoes" style="margin-top:16px;">
            <button class="sec" type="button" onclick="location.href='/app-login?app=1&v=20260528-offline1'">Tentar login online</button>
            <button class="danger" type="button" onclick="limparOffline()">Remover dados offline deste dispositivo</button>
        </div>
        <div id="status" class="status"></div>
    </main>
<script>
(function(){
    const KEY = 'voleitable_offline_partidas';
    const SESSION_KEY = 'voleitable_offline_sessao';
    const lista = document.getElementById('lista');
    const status = document.getElementById('status');

    function lerJSON(chave, padrao){
        try { return JSON.parse(localStorage.getItem(chave) || JSON.stringify(padrao)); }
        catch(e){ return padrao; }
    }

    function salvarJSON(chave, valor){ localStorage.setItem(chave, JSON.stringify(valor)); }

    window.limparOffline = function(){
        if (!confirm('Remover todas as partidas offline deste dispositivo?')) return;
        localStorage.removeItem(KEY);
        localStorage.removeItem(SESSION_KEY);
        Object.keys(localStorage).forEach(k => {
            if (k.indexOf('fila_offline_jogo_') === 0 || k.indexOf('voleitable_offline_') === 0) localStorage.removeItem(k);
        });
        render();
    };

    function render(){
        const sessao = lerJSON(SESSION_KEY, null);
        const partidas = lerJSON(KEY, []);
        lista.innerHTML = '';
        if (!partidas.length) {
            lista.innerHTML = '<div class="vazio">Nenhuma partida offline encontrada. Entre online em casa e clique em “Preparar modo offline”.</div>';
        } else {
            partidas.forEach(p => {
                const div = document.createElement('div');
                div.className = 'partida';
                const url = p.url_jogo || p.url || p.href || '#';
                div.innerHTML = `
                    <div class="titulo">${p.equipe_a || 'Equipe A'} x ${p.equipe_b || 'Equipe B'}</div>
                    <div class="meta">Competição: ${p.competicao || '-'} • Jogo ${p.ordem || p.id || '-'} • ${p.grupo ? 'Grupo ' + p.grupo + ' • ' : ''}${p.quadra ? 'Quadra ' + p.quadra : ''}</div>
                    <div class="meta">Baixada em: ${p.baixada_em ? new Date(p.baixada_em).toLocaleString('pt-BR') : '-'}</div>
                    <div class="acoes"><a class="btn primario" href="${url}">Entrar offline</a></div>
                `;
                lista.appendChild(div);
            });
        }
        if (sessao) {
            status.textContent = `Sessão offline autorizada para ${sessao.nome || sessao.usuario || 'apontador'} em ${sessao.criada_em ? new Date(sessao.criada_em).toLocaleString('pt-BR') : '-'}.`;
        } else {
            status.textContent = 'Nenhuma sessão offline autorizada encontrada.';
        }
    }
    render();
})();
</script>
</body>
</html>
    """
    resposta = make_response(html)
    resposta.headers["Content-Type"] = "text/html; charset=utf-8"
    resposta.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
    return resposta


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
    resposta.headers["Cache-Control"] = (
        "no-cache, no-store, must-revalidate, max-age=0"
    )
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
    resposta.headers["Cache-Control"] = (
        "no-cache, no-store, must-revalidate, max-age=0"
    )
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