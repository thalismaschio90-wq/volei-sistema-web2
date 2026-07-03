// static/js/competicoes.js
// Funções leves da tela de configuração da competição.
(function(){
'use strict';

window.removerCampoDestaque = function(btn){
    const row = btn && btn.closest ? btn.closest('.destaque-campo-row') : null;
    if(row) row.remove();
};

window.adicionarCampoDestaque = function(){
    const lista = document.getElementById('listaCamposDestaques');
    if(!lista) return;
    const div = document.createElement('div');
    div.className = 'destaque-campo-row';
    div.style.cssText = 'border:1px solid #d7e1ee;border-radius:14px;padding:12px;background:#fbfdff;display:grid;grid-template-columns:1.4fr .7fr .7fr .9fr auto;gap:10px;align-items:end;';
    div.innerHTML = `
        <div><label>Nome do destaque</label><input name="destaque_campo_titulo[]" placeholder="Ex.: Melhor defesa"></div>
        <div><label>Série</label><select name="destaque_campo_serie[]"><option value="">Todas</option><option value="Ouro">Ouro</option><option value="Prata">Prata</option><option value="Bronze">Bronze</option></select></div>
        <div><label>Momento</label><select name="destaque_campo_fase[]"><option value="">Final da competição</option><option value="Final">Final</option><option value="Semifinal">Semifinal</option><option value="Quartas">Quartas</option><option value="Oitavas">Oitavas</option><option value="Classificatórias">Classificatórias</option></select></div>
        <div><label>Equipes aptas</label><select name="destaque_campo_aptos[]"><option value="top3">3 primeiros</option><option value="top2">Finalistas</option><option value="top1">Campeão</option><option value="top4">4 primeiros</option><option value="todos">Todos da série</option></select></div>
        <button type="button" class="btn-secundario" onclick="removerCampoDestaque(this)">Remover</button>`;
    lista.appendChild(div);
};

function initCompeticoes(){
    const usarRodadas = document.getElementById('usarRodadasProgramadas');
    const camposRodadas = document.getElementById('rodadasProgramadasCampos');
    if (usarRodadas && camposRodadas && usarRodadas.dataset.vtpBound !== '1') {
        usarRodadas.dataset.vtpBound = '1';
        usarRodadas.addEventListener('change', function(){ camposRodadas.style.display = this.checked ? 'block' : 'none'; });
    }

    const buttons = document.querySelectorAll('.competicao-tab-btn');
    const panes = document.querySelectorAll('.competicao-tab-pane');
    const params = new URLSearchParams(window.location.search);
    const tabParam = params.get('tab') || 'dados';
    const editParam = params.get('edit') || '';
    const tabMap = {
        dados: 'tab-dados', quadras: 'tab-quadras', estrutura: 'tab-estrutura', regras: 'tab-regras',
        pontuacao: 'tab-pontuacao', classificacao: 'tab-pontuacao', avanco: 'tab-avanco-chaveamento',
        rodadas: 'tab-rodadas', destaques: 'tab-destaques', fases: 'tab-avanco-chaveamento'
    };

    function ativarTabPorId(target) {
        if (!target) return;
        buttons.forEach((btn) => btn.classList.remove('active'));
        panes.forEach((pane) => pane.classList.remove('active'));
        const botao = Array.from(buttons).find((btn) => btn.getAttribute('data-tab') === target);
        const pane = document.getElementById(target);
        if (botao) botao.classList.add('active');
        if (pane) pane.classList.add('active');
        try {
            const etapa = botao ? (botao.getAttribute('data-etapa') || '') : '';
            if(etapa){
                const url = new URL(window.location.href);
                url.searchParams.set('tab', etapa);
                history.replaceState(null, '', url.toString());
            }
        } catch(e) {}
    }

    function aplicarBloqueioEtapas() {
        panes.forEach((pane) => {
            const etapa = pane.getAttribute('data-etapa') || '';
            const salva = pane.getAttribute('data-salva') === '1';
            const editando = editParam === etapa || (etapa === 'pontuacao' && editParam === 'classificacao');
            if (!salva || editando) return;
            pane.classList.add('etapa-bloqueada');
            pane.querySelectorAll('input, select, textarea, button').forEach((el) => {
                if (el.closest('.etapa-lock-box')) return;
                el.disabled = true;
            });
        });
    }

    ativarTabPorId(tabMap[tabParam] || 'tab-dados');
    aplicarBloqueioEtapas();

    buttons.forEach((button) => {
        if(button.dataset.vtpTabsBound === '1') return;
        button.dataset.vtpTabsBound = '1';
        button.addEventListener('click', function () {
            const target = this.getAttribute('data-tab');
            ativarTabPorId(target);
        });
    });

    const listaCriterios = document.getElementById('criterios-classificacao-lista');
    const inputCriterios = document.getElementById('criterios_ordenados');
    const previewCriterios = document.getElementById('criterios-classificacao-preview');
    let criterioArrastado = null;

    function atualizarCriteriosClassificacao() {
        if (!listaCriterios || !inputCriterios) return;
        const itens = Array.from(listaCriterios.querySelectorAll('[data-criterio]'));
        const valores = [];
        const nomes = [];
        let posicaoSorteio = null;
        itens.forEach((item, index) => {
            const criterio = item.getAttribute('data-criterio');
            const titulo = item.querySelector('.criterio-titulo');
            const ordem = item.querySelector('.criterio-ordem');
            const nomeTitulo = titulo ? titulo.textContent.trim() : criterio;
            item.classList.remove('criterio-ignorado');
            if (!criterio) return;
            valores.push(criterio);
            nomes.push(`${index + 1}º ${nomeTitulo}`);
            if (ordem) ordem.textContent = index + 1;
            if (criterio === 'sorteio') posicaoSorteio = index + 1;
        });
        inputCriterios.value = valores.join(',');
        if (previewCriterios) {
            let texto = nomes.join(' → ');
            if (posicaoSorteio) texto += `. O sorteio encerra o desempate efetivo na ${posicaoSorteio}ª posição.`;
            previewCriterios.textContent = texto;
        }
    }

    if (listaCriterios && listaCriterios.dataset.vtpCriteriaBound !== '1') {
        listaCriterios.dataset.vtpCriteriaBound = '1';
        listaCriterios.querySelectorAll('[data-criterio]').forEach((item) => {
            item.setAttribute('draggable', 'true');
            item.addEventListener('dragstart', () => { criterioArrastado = item; item.classList.add('dragging'); });
            item.addEventListener('dragend', () => { item.classList.remove('dragging'); criterioArrastado = null; atualizarCriteriosClassificacao(); });
            item.addEventListener('dragover', (event) => {
                event.preventDefault();
                if (!criterioArrastado || criterioArrastado === item) return;
                const rect = item.getBoundingClientRect();
                const depois = event.clientY > rect.top + rect.height / 2;
                listaCriterios.insertBefore(criterioArrastado, depois ? item.nextSibling : item);
            });
        });
        atualizarCriteriosClassificacao();
    }
}

document.addEventListener('DOMContentLoaded', initCompeticoes);
window.VTP_Competicoes_init = initCompeticoes;
})();
