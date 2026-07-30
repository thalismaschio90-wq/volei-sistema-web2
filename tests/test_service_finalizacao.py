import pytest

from services.apontadores.finalizacao import (
    confirmar_sets,
    contexto_observacoes,
    eventos_processados_com_sucesso,
    preparar_formulario_finalizacao,
    resposta_entre_sets,
    separar_eventos_pendentes,
)


def test_separa_eventos_pendentes_sem_repetir_sincronizados():
    eventos = [{"id_local": "a"}, {"id_local": "b"}, {"tipo": "legado"}]
    pendentes, ids = separar_eventos_pendentes(eventos, {"a"})
    assert ids == ["a", "b"]
    assert pendentes == [{"id_local": "b"}, {"tipo": "legado"}]


def test_filtra_eventos_confirmados():
    ok = eventos_processados_com_sucesso([{"id_local": "a"}, {"id_local": "b"}], [{"ok": True}, {"ok": False}])
    assert ok == [{"id_local": "a"}]


def test_confirmar_sets_detecta_divergencia():
    with pytest.raises(RuntimeError):
        confirmar_sets({"sets_a": 2, "sets_b": 1}, {"sets_a": 1, "sets_b": 1})


def test_resposta_entre_sets_nao_abre_observacoes():
    resposta = resposta_entre_sets({"fim_jogo": True}, "/papeleta", [])
    assert resposta["redirecionar_papeleta"] is True
    assert resposta["abrir_observacoes"] is False
    assert resposta["estado"]["status_jogo"] == "entre_sets"


def test_contexto_observacoes_bloqueia_estado_antigo():
    contexto = contexto_observacoes({"partida": {"status_jogo": "entre_sets"}}, {}, {}, "Copa")
    assert contexto["finalizada"] is False


def test_prepara_formulario_finalizacao():
    obs, destaque = preparar_formulario_finalizacao({"observacoes": "  ok ", "destaque_lado": "a"})
    assert obs == "ok"
    assert destaque["lado"] == "A"
