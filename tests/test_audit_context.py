from core.audit_context import (
    definir_contexto_auditoria,
    enriquecer_detalhes_auditoria,
    fingerprint,
    limpar_contexto_auditoria,
    montar_contexto_auditoria,
)


def test_contexto_auditoria_nao_expoe_ip_ou_user_agent():
    contexto = montar_contexto_auditoria(
        usuario="operador",
        nome="Maria",
        perfil="apontador",
        ip="192.168.1.10",
        user_agent="Browser Teste",
        endpoint="apontadores.registrar_ponto",
        metodo="POST",
        caminho="/apontador/ponto",
    )
    assert contexto["usuario"] == "operador"
    assert contexto["ip_fingerprint"] == fingerprint("192.168.1.10")
    assert contexto["dispositivo_fingerprint"] == fingerprint("Browser Teste")
    assert "192.168.1.10" not in str(contexto)
    assert "Browser Teste" not in str(contexto)


def test_enriquecer_detalhes_preserva_existente_e_adiciona_auditoria():
    token = definir_contexto_auditoria({"usuario": "andre", "perfil": "apontador", "request_id": "r1"})
    try:
        resultado = enriquecer_detalhes_auditoria({"tipo": "ponto"})
    finally:
        limpar_contexto_auditoria(token)
    assert resultado["tipo"] == "ponto"
    assert resultado["auditoria"]["usuario"] == "andre"
    assert resultado["auditoria"]["request_id"] == "r1"


def test_enriquecer_sem_request_marca_origem_sistema():
    limpar_contexto_auditoria()
    resultado = enriquecer_detalhes_auditoria({})
    assert resultado["auditoria"]["origem"] == "sistema"
    assert resultado["auditoria"]["request_id"]
