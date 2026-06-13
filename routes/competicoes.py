from flask import Blueprint, render_template, session, redirect, url_for, request, flash

from banco import (
    conectar,
    listar_competicoes,
    buscar_competicao_por_organizador,
    competicao_existe,
    criar_competicao_com_organizador,
    excluir_competicao,
    atualizar_dados_competicao,
    atualizar_estrutura_competicao,
    atualizar_regras_jogo,
    atualizar_pontuacao_desempate,
    redefinir_senha_organizador,
    competicao_esta_travada,
    destravar_competicao,
    listar_quadras_competicao,
    garantir_quadras_competicao,
    salvar_quadras_competicao,
    buscar_configuracao_avancada_competicao,
    atualizar_configuracao_avancada_competicao,
    inicializar_configuracao_avancada_competicao,
    buscar_configuracao_agenda_competicao,
    atualizar_configuracao_agenda_competicao,
    inicializar_configuracao_agenda_competicao,
    buscar_avanco_config_competicao,
    salvar_avanco_config_competicao,
    listar_origens_avanco_competicao,
    gerar_partidas_avanco_competicao,
    status_avanco_classificatorias_competicao,
    avanco_ja_gerado_competicao,
)

from routes.utils import exigir_perfil, perfil_atual

competicoes_bp = Blueprint("competicoes", __name__)


def _competicao_do_organizador_logado():
    usuario = session.get("usuario")
    if not usuario:
        return None
    return buscar_competicao_por_organizador(usuario)


def _to_int(valor, padrao=0, minimo=None):
    try:
        numero = int(valor)
    except (TypeError, ValueError):
        numero = padrao

    if minimo is not None and numero < minimo:
        numero = minimo

    return numero


def _to_int_ou_none(valor):
    try:
        numero = int(valor)
        return numero if numero > 0 else None
    except (TypeError, ValueError):
        return None


def _injetar_trava_avanco_html(html, bloqueios=None):
    """Aplica trava visual por confronto dentro da tela /competicoes.

    Importante: NÃO bloqueia a aba inteira. Apenas os cards que já têm partida
    iniciada/finalizada ficam bloqueados. Jogos aguardando continuam editáveis.
    A proteção real contra alteração de jogos bloqueados também acontece no
    POST, preservando os confrontos bloqueados antes de salvar.
    """
    import json

    bloqueios_json = json.dumps(bloqueios or {}, ensure_ascii=False, default=str)
    aviso = f"""
<script>
(function(){{
    const BLOQUEIOS_AVANCO = {bloqueios_json};
    const MSG_PADRAO = 'Este confronto está bloqueado porque a partida já foi iniciada ou finalizada. Os demais confrontos aguardando continuam liberados.';

    function normalizarSerie(txt){{
        txt = String(txt || '').toLowerCase();
        if(txt.includes('prata')) return 'prata';
        if(txt.includes('bronze')) return 'bronze';
        return 'ouro';
    }}

    function serieAtiva(){{
        const ativo = document.querySelector('.avanco-tab.ativa, .tab-serie.ativa, .serie-tab.ativa, button.ativa');
        return normalizarSerie(ativo ? (ativo.innerText || ativo.textContent) : 'ouro');
    }}

    function texto(el){{ return (el && (el.innerText || el.textContent) || '').trim(); }}

    function jogoIdDoElemento(el){{
        const t = texto(el);
        const achados = t.match(/\bJ\d+\b/gi);
        if(achados && achados.length) return achados[0].toUpperCase();
        const dataId = el && (el.dataset.jogoId || el.dataset.id || el.getAttribute('data-jogo-id') || el.getAttribute('data-id'));
        return dataId ? String(dataId).toUpperCase() : '';
    }}

    function bloqueioPara(serie, jogoId){{
        if(!jogoId) return null;
        const keys = [
            `${{serie}}:${{jogoId}}`,
            `${{serie}}:${{jogoId}}`.toLowerCase(),
            `avanco:${{serie}}:${{jogoId}}`,
            `avanco:${{serie}}:${{jogoId}}`.toLowerCase(),
        ];
        for(const k of keys){{ if(BLOQUEIOS_AVANCO[k]) return BLOQUEIOS_AVANCO[k]; }}
        return null;
    }}

    function cardDoEvento(ev){{
        const alvo = ev.target;
        if(!alvo || !alvo.closest) return null;
        return alvo.closest('.avanco-card-jogo, .avanco-card, [data-jogo-id], [data-id]');
    }}

    function marcarCards(){{
        document.querySelectorAll('.avanco-card-jogo, .avanco-card, [data-jogo-id], [data-id]').forEach(card => {{
            const jid = jogoIdDoElemento(card);
            const info = bloqueioPara(serieAtiva(), jid);
            if(!info) return;
            card.classList.add('bloqueado');
            card.style.cursor = 'not-allowed';
            card.title = 'Confronto bloqueado: ' + (info.motivo || 'partida já iniciada ou finalizada');
            if(!card.querySelector('.avanco-card-lock-runtime')){{
                const lock = document.createElement('div');
                lock.className = 'avanco-card-lock-runtime';
                lock.style.cssText = 'position:absolute;top:8px;left:10px;font-size:11px;font-weight:1000;color:#991b1b;background:#fee2e2;border:1px solid #fecaca;border-radius:999px;padding:2px 7px;z-index:2;';
                lock.textContent = '🔒';
                card.appendChild(lock);
            }}
        }});
    }}

    document.addEventListener('click', function(ev){{
        const card = cardDoEvento(ev);
        if(!card) return;
        const jid = jogoIdDoElemento(card);
        const info = bloqueioPara(serieAtiva(), jid);
        if(!info) return;
        ev.preventDefault();
        ev.stopPropagation();
        alert((info.motivo ? 'Confronto bloqueado: ' + info.motivo + '. ' : '') + MSG_PADRAO);
        return false;
    }}, true);

    function aplicar(){{ marcarCards(); }}
    if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', aplicar);
    else aplicar();
    setTimeout(aplicar, 100);
    setTimeout(aplicar, 500);
    setInterval(aplicar, 1500);
}})();
</script>
"""
    if not isinstance(html, str):
        return html
    if "</body>" in html:
        return html.replace("</body>", aviso + "\n</body>")
    if "</html>" in html:
        return html.replace("</html>", aviso + "\n</html>")
    return html + aviso


CRITERIOS_CLASSIFICACAO_PADRAO = [
    "pontos",
    "vitorias",
    "saldo_sets",
    "sets_average",
    "saldo_pontos",
    "pontos_average",
    "confronto_direto",
    "sorteio",
]

CRITERIOS_CLASSIFICACAO_PERMITIDOS = {
    "pontos",
    "vitorias",
    "sets_average",
    "pontos_average",
    "saldo_sets",
    "saldo_pontos",
    "sets_pro",
    "sets_contra",
    "pontos_pro",
    "pontos_contra",
    "confronto_direto",
    "coef_sets",
    "coef_pontos",
    "fair_play",
    "menor_wo",
    "sorteio",
}


def _normalizar_criterios_classificacao_form(valor):
    criterios = []
    vistos = set()

    for item in str(valor or "").split(","):
        criterio = item.strip().lower().replace("-", "_").replace(" ", "_")
        if criterio in CRITERIOS_CLASSIFICACAO_PERMITIDOS and criterio not in vistos:
            criterios.append(criterio)
            vistos.add(criterio)

    if not criterios:
        criterios = list(CRITERIOS_CLASSIFICACAO_PADRAO)

    # IMPORTANTE:
    # Não removemos mais os critérios que ficarem abaixo de "sorteio".
    # A ordem completa precisa ficar salva para a tela continuar mostrando
    # todos os critérios disponíveis para o organizador arrastar/reordenar.
    #
    # O "sorteio" continua encerrando o cálculo efetivo da classificação
    # dentro do motor da tabela, mas isso NÃO deve apagar nem esconder
    # os critérios posicionados abaixo dele na configuração.

    return ",".join(criterios)


def _coletar_quadras_form():
    ids = request.form.getlist("quadra_id[]")
    nomes = request.form.getlist("quadra_nome[]")
    locais = request.form.getlist("quadra_local[]")
    ordens = request.form.getlist("quadra_ordem[]")

    quadras = []
    total = max(len(nomes), len(locais), len(ordens), len(ids))

    for idx in range(total):
        quadra_id = _to_int_ou_none(ids[idx] if idx < len(ids) else None)
        nome = (nomes[idx] if idx < len(nomes) else "").strip()
        local = (locais[idx] if idx < len(locais) else "").strip()
        ordem = _to_int(ordens[idx] if idx < len(ordens) else None, padrao=idx + 1, minimo=1)

        # Uma linha totalmente vazia não entra. Isso permite remover quadras novas sem salvar sujeira.
        if not nome and not local and not quadra_id:
            continue

        if not nome:
            nome = f"Quadra {ordem}"

        chave_ativa = f"quadra_ativa_{quadra_id}" if quadra_id else f"quadra_ativa_nova_{idx}"
        ativa = request.form.get(chave_ativa) == "on"

        quadras.append({
            "id": quadra_id,
            "nome": nome,
            "local": local,
            "ordem": ordem,
            "ativa": ativa,
        })

    return sorted(quadras, key=lambda q: q.get("ordem") or 9999)


@competicoes_bp.route("/competicoes")
@exigir_perfil("superadmin", "organizador")
def listar_competicoes_view():
    perfil = perfil_atual()

    if perfil == "superadmin":
        competicoes = listar_competicoes()
        credenciais = session.pop("credenciais_novas", None)
        senha_redefinida = session.pop("senha_redefinida_organizador", None)

        return render_template(
            "competicoes.html",
            competicoes=competicoes,
            credenciais=credenciais,
            senha_redefinida=senha_redefinida,
        )

    if perfil == "organizador":
        competicao = _competicao_do_organizador_logado()

        if not competicao:
            flash("Nenhuma competição vinculada a este organizador.", "erro")
            return redirect(url_for("painel.inicio"))

        quadras = garantir_quadras_competicao(
            competicao["nome"],
            _to_int(competicao.get("qtd_quadras"), padrao=1, minimo=1),
        )

        inicializar_configuracao_avancada_competicao(competicao["nome"])
        inicializar_configuracao_agenda_competicao(competicao["nome"])
        config = buscar_configuracao_avancada_competicao(competicao["nome"]) or {}
        config_agenda = buscar_configuracao_agenda_competicao(competicao["nome"]) or {}
        fases = config.get("fases_config") or {}

        avanco = buscar_avanco_config_competicao(competicao["nome"])
        avanco_status = status_avanco_classificatorias_competicao(competicao["nome"])
        avanco_status["gerado"] = avanco_ja_gerado_competicao(competicao["nome"])
        origens = listar_origens_avanco_competicao(competicao["nome"])

        # A aba Avanço também aparece dentro de /competicoes.
        # Por isso a trava precisa ser enviada para esta tela principal,
        # não apenas para /competicoes/avanco.
        avanco_bloqueios = buscar_bloqueios_avanco_competicao(competicao["nome"])
        # Não bloqueia o avanço inteiro. O bloqueio é por confronto.
        avanco_bloqueado = False
        if isinstance(avanco, dict):
            avanco["bloqueado"] = False
            avanco["bloqueios"] = avanco_bloqueios

        html = render_template(
            "editar_competicao.html",
            competicao=competicao,
            quadras=quadras,
            config=config,
            config_agenda=config_agenda,
            fases=fases,
            avanco=avanco,
            avanco_status=avanco_status,
            origens=origens,
            avanco_bloqueios=avanco_bloqueios,
            avanco_bloqueado=avanco_bloqueado,
            competicao_travada=competicao_esta_travada(competicao["nome"]),
        )
        # A trava visual agora é feita diretamente pelo template, por jogo.
        # Não injetamos JavaScript por fora porque isso confundia J1/J5/J7
        # pelo texto do card e acabava bloqueando confrontos aguardando.
        return html

    return redirect(url_for("painel.inicio"))


@competicoes_bp.route("/competicoes/nova", methods=["GET", "POST"])
@exigir_perfil("superadmin")
def nova_competicao():
    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        data = request.form.get("data", "").strip()
        status = request.form.get("status", "").strip() or "Em preparação"
        modo_operacao = request.form.get("modo_operacao", "simples").strip() or "simples"

        tempos_por_set = _to_int(request.form.get("tempos_por_set"), padrao=2, minimo=0)
        substituicoes_por_set = _to_int(request.form.get("substituicoes_por_set"), padrao=6, minimo=0)

        if not nome:
            flash("Informe o nome da competição.", "erro")
            return render_template("nova_competicao.html")

        if competicao_existe(nome):
            flash("Já existe uma competição com esse nome.", "erro")
            return render_template("nova_competicao.html")

        credenciais = criar_competicao_com_organizador(
            nome,
            data,
            status,
            modo_operacao=modo_operacao,
            tempos_por_set=tempos_por_set,
            substituicoes_por_set=substituicoes_por_set,
        )

        session["credenciais_novas"] = {
            "competicao": nome,
            "login": credenciais["login"],
            "senha": credenciais["senha"],
        }

        flash("Competição criada com sucesso.", "sucesso")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    return render_template("nova_competicao.html")


@competicoes_bp.route("/competicoes/salvar", methods=["POST"])
@exigir_perfil("organizador")
def salvar_competicao_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. Os dados não podem mais ser alterados.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    nome_atual = comp["nome"]

    dados = {
        "nome": request.form.get("nome", "").strip(),
        "cidade": request.form.get("cidade", "").strip(),
        "data": request.form.get("data", "").strip(),
        "ginasio": request.form.get("ginasio", "").strip(),
        "categoria": request.form.get("categoria", "").strip(),
        "sexo": request.form.get("sexo", "").strip(),
        "divisao": request.form.get("divisao", "").strip(),
        "status": request.form.get("status", "").strip() or comp.get("status", "Em preparação"),
    }

    if not dados["nome"]:
        flash("Informe o nome da competição.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    if dados["nome"] != nome_atual and competicao_existe(dados["nome"]):
        flash("Já existe uma competição com esse nome.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    atualizar_dados_competicao(nome_atual, dados)

    session["competicao"] = dados["nome"]

    flash("Dados da competição salvos com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/<nome>/excluir", methods=["POST"])
@exigir_perfil("superadmin")
def excluir_competicao_view(nome):
    confirmacao = (request.form.get("confirmacao_exclusao") or "").strip().upper()

    if confirmacao and confirmacao != "EXCLUIR":
        flash("Confirmação inválida. Digite EXCLUIR para confirmar a exclusão.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    try:
        sucesso = excluir_competicao(nome)

        if sucesso:
            flash(
                "Competição excluída com sucesso. SUPERADMIN foi preservado e os apontadores foram mantidos sem vínculo com a competição removida.",
                "sucesso",
            )
        else:
            flash("Não foi possível excluir a competição.", "erro")

    except Exception as e:
        flash(f"Erro ao excluir competição: {str(e)}", "erro")

    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/<nome>/resetar-senha", methods=["POST"])
@exigir_perfil("superadmin")
def resetar_senha_organizador_view(nome):
    competicoes = listar_competicoes()
    comp = next((c for c in competicoes if c["nome"] == nome), None)

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    resultado = redefinir_senha_organizador(comp["organizador_login"])

    session["senha_redefinida_organizador"] = {
        "competicao": nome,
        "login": resultado["login"],
        "senha": resultado["senha"],
    }

    flash("Senha do organizador redefinida com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/regras", methods=["GET", "POST"])
@exigir_perfil("organizador")
def salvar_regras_jogo_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if request.method == "GET":
        return redirect(url_for("competicoes.listar_competicoes_view"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. As regras do jogo não podem mais ser alteradas.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    dados = {
        "sets_tipo": request.form.get("sets_tipo", "melhor_de_3"),
        "pontos_set": _to_int(request.form.get("pontos_set"), padrao=25, minimo=1),
        "tem_tiebreak": request.form.get("tem_tiebreak") == "on",
        "pontos_tiebreak": _to_int(request.form.get("pontos_tiebreak"), padrao=15, minimo=1),
        "diferenca_minima": _to_int(request.form.get("diferenca_minima"), padrao=2, minimo=1),
        "tempos_por_set": _to_int(request.form.get("tempos_por_set"), padrao=2, minimo=0),
        "substituicoes_por_set": _to_int(request.form.get("substituicoes_por_set"), padrao=6, minimo=0),
    }

    atualizar_regras_jogo(comp["nome"], dados)

    flash("Regras do jogo salvas.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/estrutura", methods=["POST"])
@exigir_perfil("organizador")
def salvar_estrutura_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. A estrutura não pode mais ser alterada.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    data_limite_inscricao = request.form.get("data_limite_inscricao", "").strip()
    hora_limite_inscricao = request.form.get("hora_limite_inscricao", "").strip()

    modo_operacao = (request.form.get("modo_operacao") or comp.get("modo_operacao") or "simples").strip().lower()
    if modo_operacao not in ("simples", "avancado"):
        modo_operacao = "simples"

    dados = {
        "qtd_equipes": _to_int(request.form.get("qtd_equipes"), padrao=0, minimo=0),
        "tem_grupos": request.form.get("tem_grupos") == "on",
        "qtd_grupos": _to_int(request.form.get("qtd_grupos"), padrao=0, minimo=0),
        "modo_operacao": modo_operacao,
        "data_limite_inscricao": data_limite_inscricao,
        "hora_limite_inscricao": hora_limite_inscricao,
        "limite_atletas": _to_int(request.form.get("limite_atletas"), padrao=0, minimo=0),
        "permitir_edicao_pos_prazo": request.form.get("permitir_edicao_pos_prazo") == "on",
        "bloquear_apos_inicio": not bool(data_limite_inscricao),
    }

    atualizar_estrutura_competicao(comp["nome"], dados)

    flash("Estrutura da competição salva com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view", tab="estrutura"))



@competicoes_bp.route("/competicoes/quadras", methods=["POST"])
@exigir_perfil("organizador")
def salvar_quadras_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. As quadras não podem mais ser alteradas.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    quadras = _coletar_quadras_form()

    if not quadras:
        flash("Cadastre pelo menos uma quadra ativa para a competição.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    qtd_quadras = len(quadras)
    salvar_quadras_competicao(comp["nome"], quadras)
    atualizar_estrutura_competicao(comp["nome"], {"qtd_quadras": qtd_quadras})

    flash("Quadras da competição salvas com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))



def _json_ou_lista_ids_competicoes(valor):
    if valor in (None, ""):
        return []
    if isinstance(valor, str):
        try:
            import json
            valor = json.loads(valor)
        except Exception:
            valor = [v.strip() for v in valor.split(",")]
    ids = []
    for item in valor or []:
        try:
            numero = int(item)
            if numero > 0 and numero not in ids:
                ids.append(numero)
        except (TypeError, ValueError):
            pass
    return ids


def _coletar_grupos_compartilhados_agenda_form():
    bruto = request.form.get("grupos_compartilhados_json") or request.form.get("grupos_compartilhados")
    if bruto:
        try:
            import json
            dados = json.loads(bruto)
            if isinstance(dados, dict):
                return {
                    str(grupo).strip().upper(): _json_ou_lista_ids_competicoes(ids)
                    for grupo, ids in dados.items()
                    if str(grupo).strip()
                }
        except Exception:
            pass

    dados = {}
    for chave, valor in request.form.items():
        if not chave.startswith("grupo_quadras_"):
            continue
        grupo = chave.replace("grupo_quadras_", "", 1).strip().upper()
        ids = _json_ou_lista_ids_competicoes(valor)
        if grupo and ids:
            dados[grupo] = ids
    return dados


@competicoes_bp.route("/competicoes/agenda", methods=["POST"])
@exigir_perfil("organizador")
def salvar_agenda_automatica_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. A agenda automática não pode mais ser alterada.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    modo = (request.form.get("modo_distribuicao") or request.form.get("modo_distribuicao_agenda") or "automatico_inteligente").strip().lower()
    rodizio = (request.form.get("rodizio_grupos") or "por_rodada").strip().lower()
    descanso = _to_int(request.form.get("descanso_minimo_jogos"), padrao=1, minimo=0)
    permitir_relaxar = request.form.get("permitir_relaxar_descanso") == "on"
    quadras_compartilhadas = _json_ou_lista_ids_competicoes(
        request.form.get("quadras_compartilhadas_json") or request.form.get("quadras_compartilhadas")
    )
    grupos_compartilhados = _coletar_grupos_compartilhados_agenda_form()

    ok = atualizar_configuracao_agenda_competicao(
        comp["nome"],
        modo_distribuicao=modo,
        descanso_minimo_jogos=descanso,
        rodizio_grupos=rodizio,
        permitir_relaxar_descanso=permitir_relaxar,
        grupos_compartilhados=grupos_compartilhados,
        quadras_compartilhadas=quadras_compartilhadas,
    )

    if ok:
        flash("Configuração da geração automática salva com sucesso.", "sucesso")
    else:
        flash("Não foi possível salvar a configuração da geração automática.", "erro")

    return redirect(url_for("competicoes.listar_competicoes_view", tab="agenda"))


@competicoes_bp.route("/competicoes/pontuacao", methods=["POST"])
@exigir_perfil("organizador")
def salvar_pontuacao_desempate_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. A pontuação e os critérios de classificação não podem mais ser alterados.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    sets_tipo = comp.get("sets_tipo", "melhor_de_3")
    dados = {}

    if sets_tipo == "set_unico":
        dados["vitoria_set_unico"] = _to_int(request.form.get("vitoria_set_unico"), padrao=2)
        dados["derrota_set_unico"] = _to_int(request.form.get("derrota_set_unico"), padrao=0)

    elif sets_tipo == "melhor_de_3":
        dados["vitoria_2x0"] = _to_int(request.form.get("vitoria_2x0"), padrao=3)
        dados["vitoria_2x1"] = _to_int(request.form.get("vitoria_2x1"), padrao=2)
        dados["derrota_1x2"] = _to_int(request.form.get("derrota_1x2"), padrao=1)
        dados["derrota_0x2"] = _to_int(request.form.get("derrota_0x2"), padrao=0)

    elif sets_tipo == "melhor_de_5":
        dados["vitoria_3x0"] = _to_int(request.form.get("vitoria_3x0"), padrao=3)
        dados["vitoria_3x1"] = _to_int(request.form.get("vitoria_3x1"), padrao=3)
        dados["vitoria_3x2"] = _to_int(request.form.get("vitoria_3x2"), padrao=2)
        dados["derrota_2x3"] = _to_int(request.form.get("derrota_2x3"), padrao=1)
        dados["derrota_1x3"] = _to_int(request.form.get("derrota_1x3"), padrao=0)
        dados["derrota_0x3"] = _to_int(request.form.get("derrota_0x3"), padrao=0)

    criterios_ordenados = _normalizar_criterios_classificacao_form(
        request.form.get("criterios_ordenados", "")
    )
    dados["criterios_desempate"] = criterios_ordenados

    atualizar_pontuacao_desempate(comp["nome"], dados)

    flash("Pontuação e critérios de classificação salvos.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))



@competicoes_bp.route("/competicoes/fases", methods=["POST"])
@exigir_perfil("organizador")
def salvar_fases_competicao_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. As fases não podem mais ser alteradas.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view", tab="fases"))

    tipo_confronto = request.form.get("tipo_confronto", "grupo_interno").strip() or "grupo_interno"
    tipo_classificacao = request.form.get("tipo_classificacao", "grupo").strip() or "grupo"
    cruzamentos_grupos = request.form.get("cruzamentos_grupos", "").strip()
    formato_finais = request.form.get("formato_finais", "quartas").strip() or "quartas"
    possui_bye = request.form.get("possui_bye", "nao").strip() == "sim"

    qtd_classificados = _to_int(request.form.get("qtd_classificados"), padrao=0, minimo=0)
    qtd_bye = _to_int(request.form.get("qtd_bye"), padrao=0, minimo=0)

    fase_nomes = request.form.getlist("fase_nome[]")
    fase_series = request.form.getlist("fase_serie[]")
    fase_tipos = request.form.getlist("fase_tipo[]")
    fase_origens = request.form.getlist("fase_origem[]")

    fases_personalizadas = []
    total = max(len(fase_nomes), len(fase_series), len(fase_tipos), len(fase_origens))

    for idx in range(total):
        nome = (fase_nomes[idx] if idx < len(fase_nomes) else "").strip()
        serie = (fase_series[idx] if idx < len(fase_series) else "geral").strip() or "geral"
        tipo = (fase_tipos[idx] if idx < len(fase_tipos) else "mata_mata").strip() or "mata_mata"
        origem = (fase_origens[idx] if idx < len(fase_origens) else "").strip()

        if not nome and not origem:
            continue

        fases_personalizadas.append({
            "nome": nome or f"Fase {idx + 1}",
            "serie": serie,
            "tipo": tipo,
            "origem": origem,
            "ordem": idx + 1,
        })

    config_atual = buscar_configuracao_avancada_competicao(comp["nome"]) or {}
    fases_config = config_atual.get("fases_config") or {}

    fases_config.update({
        "tipo_confronto": tipo_confronto,
        "tipo_classificacao": tipo_classificacao,
        "cruzamentos_grupos": cruzamentos_grupos,
        "formato_finais": formato_finais,
        "fases_personalizadas": fases_personalizadas,
    })

    atualizar_configuracao_avancada_competicao(
        nome_competicao=comp["nome"],
        tipo_classificacao=tipo_classificacao,
        qtd_classificados=qtd_classificados,
        formato_finais=formato_finais,
        possui_bye=possui_bye,
        qtd_bye=qtd_bye,
        fases_config=fases_config,
        tipo_confronto=tipo_confronto,
        cruzamentos_grupos=cruzamentos_grupos,
        data_limite_inscricao=comp.get("data_limite_inscricao"),
        hora_limite_inscricao=comp.get("hora_limite_inscricao"),
        bloquear_apos_inicio=comp.get("bloquear_apos_inicio", False),
    )

    flash("Classificação e avanço salvos com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view", tab="fases"))



def _bool_select_regras_avancadas(valor):
    valor = (valor or "padrao").strip().lower()
    if valor == "sim":
        return True
    if valor == "nao":
        return False
    return None


def _modo_operacao_avancado_form(valor):
    """Normaliza o modo de operação específico de grupo/fase.

    Retorna None quando o organizador quer usar o padrão geral da competição.
    """
    valor = (valor or "padrao").strip().lower()
    if valor in {"simples", "avancado"}:
        return valor
    return None


@competicoes_bp.route("/competicoes/regras-avancadas", methods=["POST"])
@exigir_perfil("organizador")
def salvar_regras_avancadas_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. As regras avançadas não podem mais ser alteradas.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view", tab="regras-avancadas"))

    config_atual = buscar_configuracao_avancada_competicao(comp["nome"]) or {}
    fases_config = config_atual.get("fases_config") or {}

    regras_grupos = {}
    for grupo in ["A", "B", "C", "D", "E", "F", "G", "H"]:
        ativo = request.form.get(f"grupo_{grupo}_ativo") == "on"
        sets_tipo = request.form.get(f"grupo_{grupo}_sets_tipo", "set_unico").strip() or "set_unico"
        pontos_set = _to_int(request.form.get(f"grupo_{grupo}_pontos_set"), padrao=0, minimo=0)
        tem_tiebreak = _bool_select_regras_avancadas(request.form.get(f"grupo_{grupo}_tem_tiebreak"))
        pontos_tiebreak = _to_int(request.form.get(f"grupo_{grupo}_pontos_tiebreak"), padrao=0, minimo=0)
        modo_operacao = _modo_operacao_avancado_form(request.form.get(f"grupo_{grupo}_modo_operacao"))

        if ativo or pontos_set or pontos_tiebreak or tem_tiebreak is not None or modo_operacao:
            item = {
                "ativo": ativo,
                "sets_tipo": sets_tipo,
            }
            if pontos_set:
                item["pontos_set"] = pontos_set
            if tem_tiebreak is not None:
                item["tem_tiebreak"] = tem_tiebreak
            if pontos_tiebreak:
                item["pontos_tiebreak"] = pontos_tiebreak
            if modo_operacao:
                item["modo_operacao"] = modo_operacao
            regras_grupos[grupo] = item

    regras_fases = {}
    for fase_id in ["grupos", "oitavas", "quartas", "semifinal", "final"]:
        ativo = request.form.get(f"fase_{fase_id}_ativo") == "on"
        sets_tipo = request.form.get(f"fase_{fase_id}_sets_tipo", "set_unico").strip() or "set_unico"
        pontos_set = _to_int(request.form.get(f"fase_{fase_id}_pontos_set"), padrao=0, minimo=0)
        tem_tiebreak = _bool_select_regras_avancadas(request.form.get(f"fase_{fase_id}_tem_tiebreak"))
        pontos_tiebreak = _to_int(request.form.get(f"fase_{fase_id}_pontos_tiebreak"), padrao=0, minimo=0)
        modo_operacao = _modo_operacao_avancado_form(request.form.get(f"fase_{fase_id}_modo_operacao"))

        if ativo or pontos_set or pontos_tiebreak or tem_tiebreak is not None or modo_operacao:
            item = {
                "ativo": ativo,
                "sets_tipo": sets_tipo,
            }
            if pontos_set:
                item["pontos_set"] = pontos_set
            if tem_tiebreak is not None:
                item["tem_tiebreak"] = tem_tiebreak
            if pontos_tiebreak:
                item["pontos_tiebreak"] = pontos_tiebreak
            if modo_operacao:
                item["modo_operacao"] = modo_operacao
            regras_fases[fase_id] = item

    series = {}
    for serie_id in ["ouro", "prata", "bronze"]:
        ativa = request.form.get(f"serie_{serie_id}_ativa") == "on"
        faixa = request.form.get(f"serie_{serie_id}_faixa", "").strip()
        if ativa or faixa:
            series[serie_id] = {
                "ativa": ativa,
                "faixa": faixa,
            }

    repescagem = {
        "ativa": request.form.get("repescagem_ativa") == "on",
        "descricao": request.form.get("repescagem_descricao", "").strip(),
    }

    fase_nomes = request.form.getlist("fase_avancada_nome[]")
    fase_series = request.form.getlist("fase_avancada_serie[]")
    fase_tipos = request.form.getlist("fase_avancada_tipo[]")
    fase_origens = request.form.getlist("fase_avancada_origem[]")

    fases_personalizadas = []
    total = max(len(fase_nomes), len(fase_series), len(fase_tipos), len(fase_origens))

    for idx in range(total):
        nome = (fase_nomes[idx] if idx < len(fase_nomes) else "").strip()
        serie = (fase_series[idx] if idx < len(fase_series) else "geral").strip() or "geral"
        tipo = (fase_tipos[idx] if idx < len(fase_tipos) else "mata_mata").strip() or "mata_mata"
        origem = (fase_origens[idx] if idx < len(fase_origens) else "").strip()

        if not nome and not origem:
            continue

        fases_personalizadas.append({
            "nome": nome or f"Fase avançada {idx + 1}",
            "serie": serie,
            "tipo": tipo,
            "origem": origem,
            "ordem": idx + 1,
        })

    fases_config["regras_avancadas"] = {
        "grupos": regras_grupos,
        "fases": regras_fases,
        "series": series,
        "repescagem": repescagem,
        "fases_personalizadas": fases_personalizadas,
    }

    atualizar_configuracao_avancada_competicao(
        nome_competicao=comp["nome"],
        tipo_classificacao=config_atual.get("tipo_classificacao") or comp.get("tipo_classificacao") or "grupo",
        qtd_classificados=config_atual.get("qtd_classificados") or comp.get("qtd_classificados") or 0,
        formato_finais=config_atual.get("formato_finais") or comp.get("formato_finais") or "quartas",
        possui_bye=config_atual.get("possui_bye") if config_atual.get("possui_bye") is not None else comp.get("possui_bye", False),
        qtd_bye=config_atual.get("qtd_bye") or comp.get("qtd_bye") or 0,
        fases_config=fases_config,
        tipo_confronto=config_atual.get("tipo_confronto") or comp.get("tipo_confronto") or "grupo_interno",
        cruzamentos_grupos=config_atual.get("cruzamentos_grupos") or comp.get("cruzamentos_grupos") or "",
        data_limite_inscricao=comp.get("data_limite_inscricao"),
        hora_limite_inscricao=comp.get("hora_limite_inscricao"),
        bloquear_apos_inicio=comp.get("bloquear_apos_inicio", False),
    )

    flash("Regras avançadas salvas com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view", tab="regras-avancadas"))






# =========================================================
# AVANÇO / CHAVEAMENTO VISUAL
# =========================================================
def _normalizar_id_avanco(texto, padrao="ouro"):
    texto = (texto or padrao).strip().lower()
    texto = texto.replace("ç", "c").replace("ã", "a").replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    texto = "_".join(texto.split())
    return texto or padrao


def _origem_avanco_form(prefixo):
    tipo = (request.form.get(f"{prefixo}_tipo") or "").strip()
    valor = (request.form.get(f"{prefixo}_valor") or "").strip()
    label = (request.form.get(f"{prefixo}_label") or "").strip()
    if not tipo and not valor and not label:
        return {}
    return {"tipo": tipo, "valor": valor, "label": label}


def _regra_avanco_form(prefixo):
    usar = request.form.get(f"{prefixo}_usar_regra") == "on"
    sets_tipo = (request.form.get(f"{prefixo}_sets_tipo") or "padrao").strip() or "padrao"
    pontos_set = (request.form.get(f"{prefixo}_pontos_set") or "").strip()
    pontos_tiebreak = (request.form.get(f"{prefixo}_pontos_tiebreak") or "").strip()
    modo_operacao = (request.form.get(f"{prefixo}_modo_operacao") or "padrao").strip() or "padrao"

    # Regra fixa do vôlei no sistema:
    # - Melhor de 3 e Melhor de 5 SEMPRE têm set desempate/tie-break.
    # - O tie-break é automaticamente de 15 pontos, salvo se o organizador
    #   informar outro valor explicitamente.
    # - Set único não tem tie-break.
    if sets_tipo in {"melhor_de_3", "melhor_de_5"}:
        tem_tiebreak = "sim"
        if not pontos_tiebreak:
            pontos_tiebreak = "15"
    elif sets_tipo == "set_unico":
        tem_tiebreak = "nao"
        pontos_tiebreak = ""
    else:
        tem_tiebreak = (request.form.get(f"{prefixo}_tem_tiebreak") or "padrao").strip() or "padrao"

    return {
        "usar_regra_propria": usar,
        "sets_tipo": sets_tipo,
        "pontos_set": pontos_set,
        "tem_tiebreak": tem_tiebreak,
        "pontos_tiebreak": pontos_tiebreak,
        "modo_operacao": modo_operacao,
    }


def _coletar_avanco_form():
    series_ids = request.form.getlist("serie_id[]")
    series_nomes = request.form.getlist("serie_nome[]")
    series_ativas = set(request.form.getlist("serie_ativa[]"))
    fases_por_serie = {}

    for chave in request.form:
        if chave.startswith("serie_fases_"):
            sid = chave.replace("serie_fases_", "", 1)
            fases_por_serie[sid] = request.form.getlist(chave)

    series = []
    total_series = max(len(series_ids), len(series_nomes))
    for idx in range(total_series):
        sid_raw = series_ids[idx] if idx < len(series_ids) else ""
        nome = (series_nomes[idx] if idx < len(series_nomes) else "").strip()
        sid = _normalizar_id_avanco(sid_raw or nome or f"serie_{idx+1}")
        if not nome:
            nome = sid.title()
        fases = fases_por_serie.get(sid) or request.form.getlist(f"serie_fases_{sid}[]")
        if not fases:
            fases = ["semifinal", "final"]
        series.append({
            "id": sid,
            "nome": nome,
            "ativa": (sid in series_ativas) or (str(idx) in series_ativas),
            "fases": fases,
            "ordem": idx + 1,
            "regra": _regra_avanco_form(f"serie_{sid}"),
        })

    jogo_ids = request.form.getlist("jogo_id[]")
    jogo_series = request.form.getlist("jogo_serie[]")
    jogo_fases = request.form.getlist("jogo_fase[]")
    jogo_ordens = request.form.getlist("jogo_ordem[]")
    jogo_datas = request.form.getlist("jogo_data_hora[]")
    jogo_quadras_id = request.form.getlist("jogo_quadra_id[]")
    jogo_quadras_nome = request.form.getlist("jogo_quadra_nome[]")
    jogo_ginasios = request.form.getlist("jogo_ginasio[]")
    prox_vencedores = request.form.getlist("jogo_proximo_vencedor[]")
    prox_perdedores = request.form.getlist("jogo_proximo_perdedor[]")

    jogos = []
    total_jogos = max(len(jogo_ids), len(jogo_series), len(jogo_fases))
    for idx in range(total_jogos):
        jid = (jogo_ids[idx] if idx < len(jogo_ids) else f"J{idx+1}").strip() or f"J{idx+1}"
        serie = _normalizar_id_avanco(jogo_series[idx] if idx < len(jogo_series) else "ouro")
        fase = (jogo_fases[idx] if idx < len(jogo_fases) else "quartas").strip() or "quartas"
        try:
            ordem = int(jogo_ordens[idx]) if idx < len(jogo_ordens) and jogo_ordens[idx] else idx + 1
        except Exception:
            ordem = idx + 1

        jogos.append({
            "id": jid,
            "serie": serie,
            "fase": fase,
            "ordem": ordem,
            "data_hora": (jogo_datas[idx] if idx < len(jogo_datas) else "").strip(),
            "quadra_id": (jogo_quadras_id[idx] if idx < len(jogo_quadras_id) else "").strip(),
            "quadra_nome": (jogo_quadras_nome[idx] if idx < len(jogo_quadras_nome) else "").strip(),
            "ginasio": (jogo_ginasios[idx] if idx < len(jogo_ginasios) else "").strip(),
            "origem_a": _origem_avanco_form(f"jogo_{idx}_a"),
            "origem_b": _origem_avanco_form(f"jogo_{idx}_b"),
            "proximo_vencedor": (prox_vencedores[idx] if idx < len(prox_vencedores) else "").strip(),
            "proximo_perdedor": (prox_perdedores[idx] if idx < len(prox_perdedores) else "").strip(),
            "regra": _regra_avanco_form(f"jogo_{idx}"),
        })

    return {"series": series, "jogos": jogos, "versao": 1}


def _colunas_tabela_avanco(nome_tabela):
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s
                """, (nome_tabela,))
                return {str(r.get("column_name") or "") for r in (cur.fetchall() or [])}
    except Exception as e:
        print(f"AVISO _colunas_tabela_avanco/{nome_tabela}:", repr(e), flush=True)
        return set()


def _int_avanco(valor, padrao=0):
    try:
        if valor in (None, ""):
            return padrao
        return int(valor)
    except Exception:
        return padrao


def _origem_avanco_partida_real(serie, jogo_id):
    return f"avanco:{_normalizar_id_avanco(serie)}:{str(jogo_id or '').strip()}"


def _chaves_bloqueio_avanco(origem):
    origem = str(origem or "").strip()
    partes = origem.split(":")
    if len(partes) < 3 or partes[0] != "avanco":
        return []
    serie = _normalizar_id_avanco(partes[1])
    jogo_id = partes[2].strip()
    return [
        origem,
        origem.lower(),
        f"{serie}:{jogo_id}",
        f"{serie}:{jogo_id}".lower(),
    ]


def _partida_avanco_bloqueada(row):
    motivos = []

    status_campos = [
        row.get("status"),
        row.get("status_jogo"),
        row.get("status_operacao"),
        row.get("fase_partida"),
    ]
    status_bloqueados = {
        "ao vivo", "ao_vivo", "em andamento", "em_andamento",
        "iniciada", "iniciado", "pausada", "pausado",
        "finalizada", "finalizado", "encerrada", "encerrado",
        "wo", "cancelada", "cancelado",
    }
    for valor in status_campos:
        texto = str(valor or "").strip().lower()
        if texto in status_bloqueados:
            motivos.append("status da partida")
            break

    for campo in ("pontos_a", "pontos_b", "placar_a", "placar_b", "sets_a", "sets_b"):
        if _int_avanco(row.get(campo), 0) > 0:
            motivos.append("placar/sets registrados")
            break

    for campo in ("set1_a", "set1_b", "set2_a", "set2_b", "set3_a", "set3_b", "set4_a", "set4_b", "set5_a", "set5_b"):
        if row.get(campo) not in (None, ""):
            motivos.append("parciais registradas")
            break

    for campo in ("vencedor", "data_fim", "finalizado_em", "tipo_encerramento"):
        if str(row.get(campo) or "").strip():
            motivos.append("resultado/finalização registrada")
            break

    if row.get("pre_jogo_iniciado_em") not in (None, ""):
        motivos.append("pré-jogo iniciado")

    if row.get("pre_jogo_finalizado") is True:
        motivos.append("pré-jogo finalizado")

    if _int_avanco(row.get("eventos_total"), 0) > 0:
        motivos.append("eventos registrados")

    return bool(motivos), ", ".join(dict.fromkeys(motivos))


def buscar_bloqueios_avanco_competicao(nome_competicao):
    """Retorna os confrontos do avanço que não podem mais ser editados.

    A chave principal é serie:jogo, por exemplo ouro:J1. Também enviamos a
    origem completa avanco:ouro:J1 para o frontend. O bloqueio é calculado por
    sinais de jogo já iniciado/finalizado: status, placar, sets, parciais,
    pré-jogo ou eventos salvos.
    """
    colunas = _colunas_tabela_avanco("partidas")
    if not colunas:
        return {}

    campos_base = ["id", "origem", "equipe_a", "equipe_b"]
    campos_opcionais = [
        "status", "status_jogo", "status_operacao", "fase", "fase_partida",
        "pontos_a", "pontos_b", "placar_a", "placar_b", "sets_a", "sets_b",
        "set1_a", "set1_b", "set2_a", "set2_b", "set3_a", "set3_b", "set4_a", "set4_b", "set5_a", "set5_b",
        "vencedor", "data_fim", "finalizado_em", "tipo_encerramento",
        "pre_jogo_iniciado_em", "pre_jogo_finalizado",
    ]
    campos = [c for c in campos_base + campos_opcionais if c in colunas]

    eventos_expr = "0 AS eventos_total"
    eventos_join = ""
    eventos_colunas = _colunas_tabela_avanco("eventos_partida")
    if eventos_colunas and "partida_id" in eventos_colunas:
        eventos_expr = "COALESCE(ev.eventos_total, 0) AS eventos_total"
        eventos_join = """
            LEFT JOIN (
                SELECT partida_id, COUNT(*) AS eventos_total
                FROM eventos_partida
                GROUP BY partida_id
            ) ev ON ev.partida_id = p.id
        """

    fases_mata_mata = "('oitavas','quartas','semifinal','semifinais','semi','terceiro_lugar','terceiro lugar','3º lugar','3 lugar','final','finais')"
    filtros_avanco = []
    if "origem" in colunas:
        filtros_avanco.append("COALESCE(p.origem, '') LIKE 'avanco:%%'")
    if "fase" in colunas:
        filtros_avanco.append(f"LOWER(COALESCE(p.fase, '')) IN {fases_mata_mata}")
    if "fase_partida" in colunas:
        filtros_avanco.append(f"LOWER(COALESCE(p.fase_partida, '')) IN {fases_mata_mata}")
    if not filtros_avanco:
        return {}

    where_avanco = " OR ".join(filtros_avanco)

    sql = f"""
        SELECT {', '.join('p.' + c for c in campos)}, {eventos_expr}
        FROM partidas p
        {eventos_join}
        WHERE p.competicao = %s
          AND ({where_avanco})
        ORDER BY p.id
    """

    bloqueios = {}
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (nome_competicao,))
                for row in cur.fetchall() or []:
                    bloqueado, motivo = _partida_avanco_bloqueada(row)
                    if not bloqueado:
                        continue
                    origem = str(row.get("origem") or "").strip()
                    info = {
                        "bloqueado": True,
                        "motivo": motivo or "Partida já iniciada ou finalizada.",
                        "partida_id": row.get("id"),
                        "origem": origem,
                        "equipe_a": row.get("equipe_a") or "",
                        "equipe_b": row.get("equipe_b") or "",
                    }
                    chaves = _chaves_bloqueio_avanco(origem)

                    # Fallback importante:
                    # Algumas partidas antigas do mata-mata foram geradas sem
                    # origem no formato avanco:serie:Jx. Nesses casos, o ID real
                    # da partida no banco costuma bater com o ID visual do avanço
                    # (ex.: partida id 5 = J5). Se não criarmos essa chave, o
                    # front não consegue bloquear o card certo e parece que a
                    # trava não funcionou.
                    if not chaves:
                        jogo_visual = f"J{row.get('id')}"
                        for serie_fallback in ("ouro", "prata", "bronze"):
                            chaves.extend([
                                f"{serie_fallback}:{jogo_visual}",
                                f"{serie_fallback}:{jogo_visual}".lower(),
                                f"avanco:{serie_fallback}:{jogo_visual}",
                                f"avanco:{serie_fallback}:{jogo_visual}".lower(),
                            ])

                    for chave in chaves:
                        bloqueios[chave] = info
    except Exception as e:
        print("AVISO buscar_bloqueios_avanco_competicao:", repr(e), flush=True)

    return bloqueios


def _preservar_jogos_avanco_bloqueados(avanco_atual, avanco_novo, bloqueios):
    if not bloqueios:
        return avanco_novo, 0

    avanco_atual = avanco_atual or {}
    avanco_novo = avanco_novo or {}
    jogos_atuais = list(avanco_atual.get("jogos") or [])
    jogos_novos = list(avanco_novo.get("jogos") or [])
    por_chave = {}

    for jogo in jogos_novos:
        chave = f"{_normalizar_id_avanco(jogo.get('serie'))}:{str(jogo.get('id') or '').strip()}".lower()
        por_chave[chave] = jogo

    preservados = 0
    for jogo_atual in jogos_atuais:
        chave = f"{_normalizar_id_avanco(jogo_atual.get('serie'))}:{str(jogo_atual.get('id') or '').strip()}".lower()
        if chave not in bloqueios:
            continue
        if chave in por_chave:
            idx = jogos_novos.index(por_chave[chave])
            jogos_novos[idx] = jogo_atual
        else:
            jogos_novos.append(jogo_atual)
        preservados += 1

    avanco_novo["jogos"] = jogos_novos
    return avanco_novo, preservados


@competicoes_bp.route("/competicoes/avanco", methods=["GET", "POST"])
@exigir_perfil("organizador")
def avanco_competicao_view():
    comp = _competicao_do_organizador_logado()
    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if request.method == "POST":
        if competicao_esta_travada(comp["nome"]):
            flash("A competição está travada. O avanço não pode ser alterado.", "erro")
            return redirect(url_for("competicoes.avanco_competicao_view"))

        bloqueios = buscar_bloqueios_avanco_competicao(comp["nome"])
        avanco_atual = buscar_avanco_config_competicao(comp["nome"])
        avanco_novo = _coletar_avanco_form()
        avanco_novo, preservados = _preservar_jogos_avanco_bloqueados(
            avanco_atual,
            avanco_novo,
            bloqueios,
        )

        salvar_avanco_config_competicao(comp["nome"], avanco_novo)
        if preservados:
            flash(f"Chaveamento salvo. {preservados} confronto(s) já iniciado(s)/finalizado(s) foram preservados e não sofreram alteração.", "sucesso")
        else:
            flash("Chaveamento de avanço salvo com sucesso.", "sucesso")
        return redirect(url_for("competicoes.avanco_competicao_view"))

    avanco = buscar_avanco_config_competicao(comp["nome"])
    avanco_status = status_avanco_classificatorias_competicao(comp["nome"])
    avanco_status["gerado"] = avanco_ja_gerado_competicao(comp["nome"])
    origens = listar_origens_avanco_competicao(comp["nome"])
    avanco_bloqueios = buscar_bloqueios_avanco_competicao(comp["nome"])
    # Não bloqueia a aba inteira: apenas cada confronto com resultado/andamento.
    avanco_bloqueado = False
    return render_template(
        "avanco_competicao.html",
        competicao=comp,
        avanco=avanco,
        avanco_status=avanco_status,
        origens=origens,
        avanco_bloqueios=avanco_bloqueios,
        avanco_bloqueado=avanco_bloqueado,
        competicao_travada=competicao_esta_travada(comp["nome"]),
    )


@competicoes_bp.route("/competicoes/avanco/gerar", methods=["POST"])
@exigir_perfil("organizador")
def gerar_avanco_competicao_view():
    comp = _competicao_do_organizador_logado()
    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))
    # Não bloqueia a geração inteira por causa de J5/J6 finalizados.
    # A proteção contra apagar jogo já iniciado/finalizado fica no banco e no
    # salvamento do avanço; os confrontos ainda aguardando (ex.: J7/J8) devem
    # continuar podendo ser gerados/atualizados.
    resultado = gerar_partidas_avanco_competicao(comp["nome"])
    if resultado.get("bloqueada"):
        flash(f"Avanço bloqueado: ainda existem {resultado.get('pendentes_classificatoria', 0)} jogo(s) classificatório(s) pendente(s). Finalize todos antes de gerar os confrontos reais.", "erro")
    else:
        flash(f"Mata-mata gerado: {resultado.get('criadas', 0)} novas, {resultado.get('atualizadas', 0)} atualizadas e {resultado.get('duplicadas_removidas', 0)} duplicada(s) removida(s).", "sucesso")
    return redirect(url_for("competicoes.avanco_competicao_view"))


@competicoes_bp.route("/competicoes/destravar", methods=["POST"])
@exigir_perfil("organizador")
def destravar_competicao_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    confirmacao = (request.form.get("confirmacao_destravar") or "").strip().upper()
    if confirmacao != "DESTRAVAR":
        flash("Confirmação inválida. Digite DESTRAVAR para liberar a competição.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    destravar_competicao(comp["nome"])
    flash("Competição destravada com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))