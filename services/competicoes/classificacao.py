
"""Interface estável para classificação e cache da competição."""
from rules.classificacao import (
    _aplicar_criterios_classificacao,
    _calcular_classificacao,
    _calcular_ou_obter_classificacao_cacheada,
    _colunas_classificacao_publica,
    _colunas_classificacao_por_criterios,
    _competicao_eh_set_unico_tabela,
    _criterios_efetivos_ate_sorteio,
    _normalizar_criterios_classificacao,
    _obter_regras_classificacao,
    _to_bool,
)

calcular_classificacao = _calcular_classificacao
calcular_ou_obter_classificacao_cacheada = _calcular_ou_obter_classificacao_cacheada
colunas_classificacao_publica = _colunas_classificacao_publica
colunas_classificacao_por_criterios = _colunas_classificacao_por_criterios
criterios_efetivos_ate_sorteio = _criterios_efetivos_ate_sorteio
competicao_eh_set_unico_tabela = _competicao_eh_set_unico_tabela
normalizar_criterios_classificacao = _normalizar_criterios_classificacao
obter_regras_classificacao = _obter_regras_classificacao
aplicar_criterios_classificacao = _aplicar_criterios_classificacao
to_bool = _to_bool
