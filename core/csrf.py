"""Proteção CSRF global, compatível com formulários e chamadas fetch/XHR legadas."""
from __future__ import annotations

import os
import re
import secrets
from functools import wraps

from flask import abort, current_app, request, session

_TOKEN_KEY = "_csrf_token"
_FORM_RE = re.compile(r"(<form\b[^>]*\bmethod=[\"']?post[\"']?[^>]*>)", re.I)
_HEAD_RE = re.compile(r"</head>", re.I)
_BODY_RE = re.compile(r"</body>", re.I)


def _env_bool(nome: str, padrao: bool = True) -> bool:
    valor = str(os.environ.get(nome, "")).strip().lower()
    if not valor:
        return padrao
    return valor in {"1", "true", "sim", "yes", "on"}


def gerar_token_csrf() -> str:
    token = session.get(_TOKEN_KEY)
    if not token:
        token = secrets.token_urlsafe(32)
        session[_TOKEN_KEY] = token
    return token


def _token_recebido() -> str:
    return str(
        request.headers.get("X-CSRF-Token")
        or request.headers.get("X-CSRFToken")
        or request.form.get("_csrf_token")
        or request.args.get("_csrf_token")
        or ""
    )


def csrf_exempt(view):
    view._csrf_exempt = True
    return view


def registrar_csrf(app) -> None:
    app.config.setdefault("CSRF_ENABLED", _env_bool("CSRF_ENABLED", True))

    @app.context_processor
    def _csrf_context():
        return {"csrf_token": gerar_token_csrf}

    @app.before_request
    def _validar_csrf():
        if not current_app.config.get("CSRF_ENABLED", True):
            return None
        if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
            return None
        endpoint = request.endpoint or ""
        if endpoint == "static":
            return None
        view = current_app.view_functions.get(endpoint)
        if view is not None and getattr(view, "_csrf_exempt", False):
            return None

        esperado = str(session.get(_TOKEN_KEY) or "")
        recebido = _token_recebido()
        if not esperado or not recebido or not secrets.compare_digest(esperado, recebido):
            abort(400, description="Token de segurança ausente ou inválido. Atualize a página e tente novamente.")
        return None

    @app.after_request
    def _injetar_csrf(response):
        if not current_app.config.get("CSRF_ENABLED", True):
            return response
        content_type = str(response.headers.get("Content-Type", "")).lower()
        if "text/html" not in content_type or response.direct_passthrough:
            return response
        try:
            html = response.get_data(as_text=True)
        except Exception:
            return response
        if not html or "<html" not in html.lower():
            return response

        token = gerar_token_csrf()
        hidden = f'<input type="hidden" name="_csrf_token" value="{token}">'

        def adicionar_campo(match):
            tag = match.group(1)
            return tag if "_csrf_token" in tag else tag + hidden

        html = _FORM_RE.sub(adicionar_campo, html)
        meta = f'<meta name="csrf-token" content="{token}">'

        script = f"""<script>(function(){{
const t={token!r};
const of=window.fetch; if(of){{window.fetch=function(i,n){{n=n||{{}}; const inferred=(i&&typeof i==='object'&&i.method)||'GET'; const method=String(n.method||inferred).toUpperCase(); if(!['GET','HEAD','OPTIONS','TRACE'].includes(method)){{const h=new Headers(n.headers||(i&&i.headers)||{{}}); if(!h.has('X-CSRF-Token'))h.set('X-CSRF-Token',t); n.headers=h;}} return of.call(this,i,n);}};}}
const o=XMLHttpRequest.prototype.open,s=XMLHttpRequest.prototype.send; XMLHttpRequest.prototype.open=function(method){{this.__vtpMethod=String(method||'GET').toUpperCase(); return o.apply(this,arguments);}}; XMLHttpRequest.prototype.send=function(){{if(!['GET','HEAD','OPTIONS','TRACE'].includes(this.__vtpMethod||'GET')){{try{{this.setRequestHeader('X-CSRF-Token',t);}}catch(e){{}}}} return s.apply(this,arguments);}};
if(navigator.sendBeacon){{const ob=navigator.sendBeacon.bind(navigator); navigator.sendBeacon=function(url,data){{try{{const u=new URL(url,window.location.href);u.searchParams.set('_csrf_token',t);return ob(u.toString(),data);}}catch(e){{return ob(url,data);}}}};}}
}})();</script>"""
        if "__vtpMethod" not in html:
            html = _HEAD_RE.sub(meta + script + "</head>", html, count=1) if 'name="csrf-token"' not in html else _HEAD_RE.sub(script + "</head>", html, count=1)
        response.set_data(html)
        response.headers["Content-Length"] = str(len(response.get_data()))
        return response
