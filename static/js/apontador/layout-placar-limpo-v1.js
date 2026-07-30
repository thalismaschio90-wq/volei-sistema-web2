(function(){
    function moverCardsEquipeParaQuadras(){
        if (window.innerWidth < 1025) return;
        var teamA = document.querySelector('.pc-scoreboard-card > .pc-team-left') || document.querySelector('.pc-team-left.pc-team-in-quadra');
        var teamB = document.querySelector('.pc-scoreboard-card > .pc-team-right') || document.querySelector('.pc-team-right.pc-team-in-quadra');
        var quadraA = document.querySelector('#quadra-a')?.closest('.quadra-box');
        var quadraB = document.querySelector('#quadra-b')?.closest('.quadra-box');
        if (teamA && quadraA && !quadraA.contains(teamA)) {
            teamA.classList.add('pc-team-in-quadra');
            quadraA.insertBefore(teamA, quadraA.firstChild);
        }
        if (teamB && quadraB && !quadraB.contains(teamB)) {
            teamB.classList.add('pc-team-in-quadra');
            quadraB.insertBefore(teamB, quadraB.firstChild);
        }
        if (typeof window.atualizarBolinhaSaqueApontador === 'function') {
            window.atualizarBolinhaSaqueApontador();
        }
    }
    document.addEventListener('DOMContentLoaded', moverCardsEquipeParaQuadras);
    window.addEventListener('resize', moverCardsEquipeParaQuadras);
    setTimeout(moverCardsEquipeParaQuadras, 80);
})();
