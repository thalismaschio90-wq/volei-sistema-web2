from realtime.synchronization import (
    emitir_para_cliente,
    inscrever_em_salas,
    montar_confirmacao,
    normalizar_entrada,
    obter_estado_inicial,
    salas_para_entrada,
)


class StoreFake:
    def __init__(self, dados):
        self.dados = dados
        self.consultadas = []

    def obter(self, chave):
        self.consultadas.append(chave)
        return self.dados.get(chave)


class SocketFake:
    def __init__(self):
        self.emissoes = []

    def emit(self, evento, payload, room=None):
        self.emissoes.append((evento, payload, room))


def test_normalizar_entrada_e_salas_sem_duplicacao():
    entrada = normalizar_entrada({
        "partida_id": " 12 ",
        "competicao": " Copa ",
        "perfil": " arbitro ",
        "room": "partida:12",
    })
    assert entrada.partida_id == "12"
    assert entrada.competicao == "Copa"
    assert entrada.perfil == "arbitro"
    salas = salas_para_entrada(entrada)
    assert salas == ["partida:12", "12", "legacy:12"]
    assert len(salas) == len(set(salas))
    assert "arbitros:Copa:12" not in salas


def test_inscrever_em_salas_usa_uma_unica_lista_canonica():
    entrada = normalizar_entrada({"partida_id": 7, "competicao": "Liga"})
    inscritas = []
    retorno = inscrever_em_salas(entrada, inscritas.append)
    assert retorno == inscritas
    assert inscritas == ["7", "legacy:7"]
    assert "partida:7" not in inscritas
    assert "partida:Liga:7" not in inscritas


def test_obter_estado_inicial_tenta_fallback_sem_expor_store():
    estado = {"pontos_a": 8}
    store = StoreFake({"alternativa": estado})
    retorno = obter_estado_inicial(store, "principal", chaves_alternativas=["alternativa"])
    assert retorno == estado
    assert store.consultadas == ["principal", "alternativa"]


def test_montar_confirmacao_preserva_perfil_e_arbitro():
    entrada = normalizar_entrada({"partida_id": "3", "competicao": "C", "perfil": "segundo"})
    payload = montar_confirmacao(entrada, room="arbitros:3", arbitro=True)
    assert payload == {
        "ok": True,
        "partida_id": "3",
        "competicao": "C",
        "room": "arbitros:3",
        "perfil": "segundo",
        "arbitro": True,
    }


def test_emitir_para_cliente_remove_eventos_repetidos():
    socket = SocketFake()
    payload = {"ok": True}
    emitir_para_cliente(socket, "sid-1", ["estado", "estado", "placar"], payload)
    assert socket.emissoes == [
        ("estado", payload, "sid-1"),
        ("placar", payload, "sid-1"),
    ]
