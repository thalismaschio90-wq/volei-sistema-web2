// static/js/tabela.js
// Controla a tabela sem recarregar toda a página a cada troca de aba.
(function(){
'use strict';

async function atualizarPartidasAoVivoTabela() {
    const linhas = document.querySelectorAll('tr[data-partida-id][data-ao-vivo="1"]');

    for (const linha of linhas) {
        const partidaId = linha.dataset.partidaId;
        const competicao = linha.dataset.competicao;

        try {
            const resp = await fetch(`/apontador/estado/${encodeURIComponent(competicao)}/${partidaId}?leve=1`, {
                cache: "no-cache"
            });

            if (!resp.ok) continue;

            const dados = await resp.json();
            if (!dados.ok) continue;

            const statusEl = document.getElementById(`status-${partidaId}`);
            const setsEl = document.getElementById(`sets-${partidaId}`);
            const parciaisEl = document.getElementById(`parciais-${partidaId}`);

            if (statusEl) {
                if (dados.partida_finalizada) {
                    statusEl.innerHTML = '<span class="badge-status badge-finalizado">FINALIZADO</span>';
                    linha.dataset.aoVivo = "0";
                } else {
                    statusEl.innerHTML = '<span class="badge-status badge-ao-vivo">AO VIVO</span>';
                }
            }

            if (setsEl) {
                setsEl.textContent = `${dados.placar_exibicao_a ?? dados.sets_a ?? 0} x ${dados.placar_exibicao_b ?? dados.sets_b ?? 0}`;
            }

            if (parciaisEl) {
                if (!dados.partida_finalizada) {
                    parciaisEl.textContent = `${dados.placar_exibicao_a ?? dados.pontos_a ?? 0} x ${dados.placar_exibicao_b ?? dados.pontos_b ?? 0}`;
                }
            }
        } catch (e) {
            console.error("Erro ao atualizar partida ao vivo:", partidaId, e);
        }
    }
}

setInterval(() => {
    if (document.visibilityState === "visible") atualizarPartidasAoVivoTabela();
}, 15000);
document.addEventListener("DOMContentLoaded", atualizarPartidasAoVivoTabela);

document.addEventListener("DOMContentLoaded", function sincronizarQuadraDoGrupo() {
    const grupoSelect = document.getElementById('grupo');
    const quadraSelect = document.getElementById('quadra_id');
    if (!grupoSelect || !quadraSelect) return;

    grupoSelect.addEventListener('change', function () {
        const option = this.options[this.selectedIndex];
        const quadraId = option ? option.getAttribute('data-quadra-id') : '';
        if (quadraId) {
            quadraSelect.value = quadraId;
        }
    });
});

document.addEventListener("DOMContentLoaded", function controlarModaisEquipeGrupo() {
    function abrirModal(id) {
        const modal = document.getElementById(id);
        if (!modal) return;

        modal.classList.add("ativo");
        modal.setAttribute("aria-hidden", "false");
        document.body.style.overflow = "hidden";

        const busca = modal.querySelector("[data-busca-equipe-modal]");
        if (busca) {
            busca.value = "";
            modal.querySelectorAll("[data-equipe-modal-item]").forEach(item => item.style.display = "");
            setTimeout(() => busca.focus(), 80);
        }
    }

    function fecharModal(modal) {
        if (!modal) return;

        modal.classList.remove("ativo");
        modal.setAttribute("aria-hidden", "true");

        if (!document.querySelector(".modal-equipes-backdrop.ativo")) {
            document.body.style.overflow = "";
        }
    }

    document.querySelectorAll("[data-abrir-modal-equipes]").forEach(botao => {
        botao.addEventListener("click", () => abrirModal(botao.dataset.abrirModalEquipes));
    });

    document.querySelectorAll("[data-fechar-modal-equipes]").forEach(botao => {
        botao.addEventListener("click", () => fecharModal(botao.closest(".modal-equipes-backdrop")));
    });

    document.querySelectorAll(".modal-equipes-backdrop").forEach(backdrop => {
        backdrop.addEventListener("click", event => {
            if (event.target === backdrop) fecharModal(backdrop);
        });
    });

    document.addEventListener("keydown", event => {
        if (event.key === "Escape") {
            document.querySelectorAll(".modal-equipes-backdrop.ativo").forEach(fecharModal);
        }
    });

    document.querySelectorAll("[data-busca-equipe-modal]").forEach(input => {
        input.addEventListener("input", () => {
            const termo = input.value.trim().toLowerCase();
            const modal = input.closest(".modal-equipes");
            if (!modal) return;

            modal.querySelectorAll("[data-equipe-modal-item]").forEach(item => {
                const nome = (item.dataset.equipeNome || "").toLowerCase();
                item.style.display = nome.includes(termo) ? "" : "none";
            });
        });
    });
});

document.addEventListener("DOMContentLoaded", function controlarFiltrosPartidas() {
    const tabela = document.querySelector("[data-tabela-partidas]");
    if (!tabela) return;

    const containerQuadras = document.querySelector("[data-filtro-quadras]");
    const containerRodadas = document.querySelector("[data-filtro-rodadas]");
    const vazio = document.querySelector("[data-quadra-filtro-vazio]");
    const linhas = tabela.querySelectorAll("tbody tr[data-partida-id]");
    let filtroQuadra = "todas";
    let filtroRodada = "todas";

    function aplicarFiltros() {
        let visiveis = 0;

        linhas.forEach(linha => {
            const quadraId = linha.dataset.quadraId || "sem_quadra";
            const rodada = linha.dataset.rodada || "sem_rodada";
            const passaQuadra = filtroQuadra === "todas" || quadraId === filtroQuadra;
            const passaRodada = filtroRodada === "todas" || rodada === filtroRodada;
            const mostrar = passaQuadra && passaRodada;
            linha.style.display = mostrar ? "" : "none";
            if (mostrar) visiveis += 1;
        });

        if (containerQuadras) {
            containerQuadras.querySelectorAll("[data-quadra-filtro]").forEach(botao => {
                botao.classList.toggle("ativa", botao.dataset.quadraFiltro === filtroQuadra);
            });
        }

        if (containerRodadas) {
            containerRodadas.querySelectorAll("[data-rodada-filtro]").forEach(botao => {
                botao.classList.toggle("ativa", botao.dataset.rodadaFiltro === filtroRodada);
            });
        }

        if (vazio) {
            vazio.style.display = visiveis ? "none" : "block";
        }
    }

    if (containerQuadras) {
        containerQuadras.querySelectorAll("[data-quadra-filtro]").forEach(botao => {
            botao.addEventListener("click", () => {
                filtroQuadra = botao.dataset.quadraFiltro || "todas";
                aplicarFiltros();
            });
        });
    }

    if (containerRodadas) {
        containerRodadas.querySelectorAll("[data-rodada-filtro]").forEach(botao => {
            botao.addEventListener("click", () => {
                filtroRodada = botao.dataset.rodadaFiltro || "todas";
                aplicarFiltros();
            });
        });
    }

    aplicarFiltros();
});


function copiarLinkVisualizador() {
    const input = document.querySelector(".tabela-link-box");
    const link = input ? input.value : window.location.href;

    navigator.clipboard.writeText(link)
        .then(() => {
            alert("Link copiado com sucesso!");
        })
        .catch(() => {
            alert("Não foi possível copiar o link.");
        });
}


function marcarBind(el, chave){
    if(!el) return false;
    const attr = 'data-vtp-bound-' + chave;
    if(el.getAttribute(attr)==='1') return false;
    el.setAttribute(attr,'1');
    return true;
}

async function carregarTabelaParcial(url){
    const app = document.querySelector('[data-tabela-app]');
    if(!app) { window.location.href = url; return; }
    const destino = new URL(url, window.location.origin);
    destino.searchParams.set('parcial','1');
    app.classList.add('carregando');
    try{
        const resp = await fetch(destino.toString(), {headers:{'X-Requested-With':'fetch'}, credentials:'same-origin', cache:'no-cache'});
        if(!resp.ok) throw new Error('HTTP '+resp.status);
        const html = await resp.text();
        app.innerHTML = html;
        history.pushState({tabelaAjax:true}, '', url);
        window.VTP_Tabela_init(app);
        window.scrollTo({top:0, behavior:'smooth'});
    }catch(e){
        console.error('Falha ao carregar aba da tabela:', e);
        window.location.href = url;
    }finally{
        app.classList.remove('carregando');
    }
}

function bindTabelaAjax(root){
    (root || document).querySelectorAll('a.tabela-tab[href]').forEach(link => {
        if(!marcarBind(link, 'ajax')) return;
        link.addEventListener('click', function(ev){
            if(ev.ctrlKey || ev.metaKey || ev.shiftKey || ev.altKey || this.target === '_blank') return;
            const url = this.href;
            if(!url || new URL(url).origin !== window.location.origin) return;
            ev.preventDefault();
            carregarTabelaParcial(url);
        });
    });
}


function sincronizarQuadraDoGrupoRoot(root){
    const grupoSelect = (root || document).querySelector('#grupo');
    const quadraSelect = (root || document).querySelector('#quadra_id');
    if (!grupoSelect || !quadraSelect || grupoSelect.dataset.vtpQuadraBound === '1') return;
    grupoSelect.dataset.vtpQuadraBound = '1';
    grupoSelect.addEventListener('change', function () {
        const option = this.options[this.selectedIndex];
        const quadraId = option ? option.getAttribute('data-quadra-id') : '';
        if (quadraId) quadraSelect.value = quadraId;
    });
}

function controlarModaisEquipeGrupoRoot(root){
    root = root || document;
    function abrirModal(id) {
        const modal = document.getElementById(id);
        if (!modal) return;
        modal.classList.add('ativo');
        modal.setAttribute('aria-hidden', 'false');
        document.body.style.overflow = 'hidden';
        const busca = modal.querySelector('[data-busca-equipe-modal]');
        if (busca) {
            busca.value = '';
            modal.querySelectorAll('[data-equipe-modal-item]').forEach(item => item.style.display = '');
            setTimeout(() => busca.focus(), 80);
        }
    }
    function fecharModal(modal) {
        if (!modal) return;
        modal.classList.remove('ativo');
        modal.setAttribute('aria-hidden', 'true');
        if (!document.querySelector('.modal-equipes-backdrop.ativo')) document.body.style.overflow = '';
    }
    root.querySelectorAll('[data-abrir-modal-equipes]').forEach(botao => {
        if(botao.dataset.vtpModalBound === '1') return;
        botao.dataset.vtpModalBound = '1';
        botao.addEventListener('click', () => abrirModal(botao.dataset.abrirModalEquipes));
    });
    root.querySelectorAll('[data-fechar-modal-equipes]').forEach(botao => {
        if(botao.dataset.vtpCloseBound === '1') return;
        botao.dataset.vtpCloseBound = '1';
        botao.addEventListener('click', () => fecharModal(botao.closest('.modal-equipes-backdrop')));
    });
    root.querySelectorAll('.modal-equipes-backdrop').forEach(backdrop => {
        if(backdrop.dataset.vtpBackdropBound === '1') return;
        backdrop.dataset.vtpBackdropBound = '1';
        backdrop.addEventListener('click', event => { if (event.target === backdrop) fecharModal(backdrop); });
    });
    root.querySelectorAll('[data-busca-equipe-modal]').forEach(input => {
        if(input.dataset.vtpBuscaBound === '1') return;
        input.dataset.vtpBuscaBound = '1';
        input.addEventListener('input', () => {
            const termo = input.value.trim().toLowerCase();
            const modal = input.closest('.modal-equipes');
            if (!modal) return;
            modal.querySelectorAll('[data-equipe-modal-item]').forEach(item => {
                const nome = (item.dataset.equipeNome || '').toLowerCase();
                item.style.display = nome.includes(termo) ? '' : 'none';
            });
        });
    });
}

function controlarFiltrosPartidasRoot(root){
    root = root || document;
    const tabela = root.querySelector('[data-tabela-partidas]');
    if (!tabela || tabela.dataset.vtpFiltrosBound === '1') return;
    tabela.dataset.vtpFiltrosBound = '1';
    const containerQuadras = root.querySelector('[data-filtro-quadras]');
    const containerRodadas = root.querySelector('[data-filtro-rodadas]');
    const vazio = root.querySelector('[data-quadra-filtro-vazio]');
    const linhas = tabela.querySelectorAll('tbody tr[data-partida-id]');
    let filtroQuadra = 'todas';
    let filtroRodada = 'todas';
    function aplicarFiltros() {
        let visiveis = 0;
        linhas.forEach(linha => {
            const quadraId = linha.dataset.quadraId || 'sem_quadra';
            const rodada = linha.dataset.rodada || 'sem_rodada';
            const passaQuadra = filtroQuadra === 'todas' || quadraId === filtroQuadra;
            const passaRodada = filtroRodada === 'todas' || rodada === filtroRodada;
            const mostrar = passaQuadra && passaRodada;
            linha.style.display = mostrar ? '' : 'none';
            if (mostrar) visiveis += 1;
        });
        if (containerQuadras) containerQuadras.querySelectorAll('[data-quadra-filtro]').forEach(botao => botao.classList.toggle('ativa', botao.dataset.quadraFiltro === filtroQuadra));
        if (containerRodadas) containerRodadas.querySelectorAll('[data-rodada-filtro]').forEach(botao => botao.classList.toggle('ativa', botao.dataset.rodadaFiltro === filtroRodada));
        if (vazio) vazio.style.display = visiveis ? 'none' : 'block';
    }
    if (containerQuadras) containerQuadras.querySelectorAll('[data-quadra-filtro]').forEach(botao => {
        botao.addEventListener('click', () => { filtroQuadra = botao.dataset.quadraFiltro || 'todas'; aplicarFiltros(); });
    });
    if (containerRodadas) containerRodadas.querySelectorAll('[data-rodada-filtro]').forEach(botao => {
        botao.addEventListener('click', () => { filtroRodada = botao.dataset.rodadaFiltro || 'todas'; aplicarFiltros(); });
    });
    aplicarFiltros();
}

if(!window.__VTP_TABELA_ESC_BOUND){
    window.__VTP_TABELA_ESC_BOUND = true;
    document.addEventListener('keydown', event => {
        if (event.key === 'Escape') {
            document.querySelectorAll('.modal-equipes-backdrop.ativo').forEach(modal => {
                modal.classList.remove('ativo');
                modal.setAttribute('aria-hidden', 'true');
            });
            document.body.style.overflow = '';
        }
    });
}

function initTabela(root){
    root = root || document;
    bindTabelaAjax(root);
    sincronizarQuadraDoGrupoRoot(root);
    controlarModaisEquipeGrupoRoot(root);
    controlarFiltrosPartidasRoot(root);
    try { atualizarPartidasAoVivoTabela(); } catch(e) {}
    // Reexecuta os inicializadores antigos em conteúdo trocado via AJAX.
    try {
        const evt = new Event('DOMContentLoaded');
        // Evita disparar globalmente em loop: os binds antigos têm data flags nos elementos principais via DOM novo.
    } catch(e) {}
}

window.VTP_Tabela_init = initTabela;
window.copiarLinkVisualizador = copiarLinkVisualizador;

document.addEventListener('DOMContentLoaded', function(){ initTabela(document); });
window.addEventListener('popstate', function(){ window.location.reload(); });
})();
