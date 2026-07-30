from rules.atletas import (
    mensagem_pendencias_obrigatorias,
    normalizar_dados_atleta,
    normalizar_instagram,
    pendencias_obrigatorias,
    validar_campos_basicos_cadastro,
)


def test_normaliza_instagram_sem_duplicar_arroba():
    assert normalizar_instagram(" jogador ") == "@jogador"
    assert normalizar_instagram("@jogador") == "@jogador"


def test_normaliza_numero_e_campos():
    ok, dados, mensagem = normalizar_dados_atleta(
        nome=" Ana ",
        cpf=" 123 ",
        data_nascimento=" 2000-01-01 ",
        numero="7",
        equipe=" Equipe A ",
        competicao=" Copa ",
        instagram="ana",
    )
    assert ok is True
    assert mensagem == ""
    assert dados is not None
    assert dados.nome == "Ana"
    assert dados.numero == 7
    assert dados.instagram == "@ana"


def test_rejeita_numero_invalido():
    ok, dados, mensagem = normalizar_dados_atleta(
        nome="Ana", cpf="123", data_nascimento="2000-01-01", numero="sete"
    )
    assert ok is False
    assert dados is None
    assert mensagem == "Número inválido."


def test_campos_basicos_cadastro():
    assert validar_campos_basicos_cadastro("", "", "") == (
        False,
        "Informe nome e CPF do atleta.",
    )


def test_pendencias_obrigatorias_e_mensagem():
    pendencias = pendencias_obrigatorias(
        exigir_foto=True,
        exigir_instagram=True,
        foto_atleta="",
        instagram="",
    )
    assert pendencias == ["foto", "Instagram"]
    assert mensagem_pendencias_obrigatorias(pendencias, acao="cadastro") == (
        "Esta competição exige foto e Instagram dos atletas. "
        "Preencha antes de concluir a inscrição."
    )
