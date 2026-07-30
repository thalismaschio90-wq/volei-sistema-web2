import re
from collections import Counter
from pathlib import Path


def test_javascript_principal_nao_declara_funcoes_duplicadas():
    caminho = (
        Path(__file__).resolve().parents[1]
        / "static"
        / "js"
        / "apontador"
        / "jogo-apontador-main.js"
    )
    conteudo = caminho.read_text(encoding="utf-8")
    nomes = re.findall(
        r"^\s*(?:async\s+)?function\s+([A-Za-z_$][\w$]*)\s*\(",
        conteudo,
        flags=re.MULTILINE,
    )
    duplicadas = sorted(nome for nome, total in Counter(nomes).items() if total > 1)
    assert duplicadas == [], f"Funções duplicadas no arquivo principal: {duplicadas}"
