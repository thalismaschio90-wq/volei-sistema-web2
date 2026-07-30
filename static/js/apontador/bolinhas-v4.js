(function(){
    function normaliza(txt){ return (txt || '').toString().trim().toLowerCase(); }
    function atualizarBolinhaSaque(){
        var info = document.getElementById('saque-info');
        var dotA = document.getElementById('saque-dot-a');
        var dotB = document.getElementById('saque-dot-b');
        var nomeA = document.getElementById('nome-a');
        var nomeB = document.getElementById('nome-b');
        if(!info || !dotA || !dotB || !nomeA || !nomeB) return;
        var texto = normaliza(info.textContent).replace('saque atual:', '').trim();
        var a = normaliza(nomeA.textContent);
        var b = normaliza(nomeB.textContent);
        dotA.classList.remove('ativo');
        dotB.classList.remove('ativo');
        if(texto && a && (texto === a || texto.indexOf(a) >= 0 || a.indexOf(texto) >= 0)) dotA.classList.add('ativo');
        if(texto && b && (texto === b || texto.indexOf(b) >= 0 || b.indexOf(texto) >= 0)) dotB.classList.add('ativo');
    }
    window.atualizarBolinhaSaqueApontador = atualizarBolinhaSaque;
    document.addEventListener('DOMContentLoaded', function(){
        atualizarBolinhaSaque();
        var info = document.getElementById('saque-info');
        if(info && 'MutationObserver' in window){
            new MutationObserver(atualizarBolinhaSaque).observe(info, {childList:true, subtree:true, characterData:true});
        }
        setInterval(atualizarBolinhaSaque, 1200);
    });
})();
