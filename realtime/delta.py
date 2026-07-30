"""Deltas compactos para o estado vivo das partidas.

O formato usa um patch recursivo simples:
- dicionários são mesclados;
- listas e valores escalares são substituídos;
- chaves removidas são listadas separadamente.

O snapshot completo continua sendo a fonte autoritativa. O delta é apenas uma
otimização de transporte entre versões consecutivas.
"""
from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from typing import Any


_SEM_ALTERACAO = object()


@dataclass(frozen=True, slots=True)
class DeltaEstado:
    partida_id: str
    versao_base: int
    versao: int
    patch: dict[str, Any]
    removidas: tuple[str, ...]
    bytes_delta: int
    bytes_estado: int

    @property
    def vazio(self) -> bool:
        return not self.patch and not self.removidas

    @property
    def economia_percentual(self) -> float:
        if self.bytes_estado <= 0:
            return 0.0
        return max(0.0, 100.0 * (1.0 - (self.bytes_delta / self.bytes_estado)))

    def payload(self) -> dict[str, Any]:
        return {
            "partida_id": self.partida_id,
            "payload_delta": True,
            "estado_versao_base": self.versao_base,
            "estado_versao": self.versao,
            "patch": copy.deepcopy(self.patch),
            "chaves_removidas": list(self.removidas),
            "bytes_delta": self.bytes_delta,
            "bytes_estado": self.bytes_estado,
        }


def _json_bytes(valor: Any) -> int:
    try:
        texto = json.dumps(valor, ensure_ascii=False, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        texto = str(valor)
    return len(texto.encode("utf-8"))


def _diff_dict(anterior: dict[str, Any], atual: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    patch: dict[str, Any] = {}
    removidas: list[str] = []

    for chave in anterior.keys() - atual.keys():
        removidas.append(str(chave))

    for chave, valor_atual in atual.items():
        if chave not in anterior:
            patch[chave] = copy.deepcopy(valor_atual)
            continue

        valor_anterior = anterior[chave]
        if isinstance(valor_anterior, dict) and isinstance(valor_atual, dict):
            subpatch, subremovidas = _diff_dict(valor_anterior, valor_atual)
            if subpatch or subremovidas:
                patch[chave] = {
                    "__vtp_patch__": subpatch,
                    "__vtp_removidas__": subremovidas,
                }
        elif valor_anterior != valor_atual:
            patch[chave] = copy.deepcopy(valor_atual)

    return patch, sorted(removidas)


def criar_delta_estado(
    partida_id: object,
    anterior: dict[str, Any] | None,
    atual: dict[str, Any] | None,
    *,
    versao_base: int = 0,
    versao: int = 0,
) -> DeltaEstado:
    estado_anterior = dict(anterior or {})
    estado_atual = dict(atual or {})
    patch, removidas = _diff_dict(estado_anterior, estado_atual)
    payload_parcial = {
        "partida_id": str(partida_id or ""),
        "payload_delta": True,
        "estado_versao_base": int(versao_base or 0),
        "estado_versao": int(versao or 0),
        "patch": patch,
        "chaves_removidas": removidas,
    }
    return DeltaEstado(
        partida_id=str(partida_id or ""),
        versao_base=int(versao_base or 0),
        versao=int(versao or 0),
        patch=patch,
        removidas=tuple(removidas),
        bytes_delta=_json_bytes(payload_parcial),
        bytes_estado=_json_bytes(estado_atual),
    )


def _aplicar_patch_dict(destino: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    resultado = copy.deepcopy(destino)
    for chave, valor in patch.items():
        if (
            isinstance(valor, dict)
            and "__vtp_patch__" in valor
            and "__vtp_removidas__" in valor
        ):
            base = resultado.get(chave)
            if not isinstance(base, dict):
                base = {}
            sub = _aplicar_patch_dict(base, dict(valor.get("__vtp_patch__") or {}))
            for removida in valor.get("__vtp_removidas__") or []:
                sub.pop(str(removida), None)
            resultado[chave] = sub
        else:
            resultado[chave] = copy.deepcopy(valor)
    return resultado


def aplicar_delta_estado(estado: dict[str, Any] | None, delta: dict[str, Any]) -> dict[str, Any]:
    """Aplica um payload de delta e devolve uma nova cópia do estado."""
    resultado = _aplicar_patch_dict(dict(estado or {}), dict(delta.get("patch") or {}))
    for chave in delta.get("chaves_removidas") or []:
        resultado.pop(str(chave), None)
    if delta.get("estado_versao") is not None:
        resultado["estado_versao"] = int(delta.get("estado_versao") or 0)
    return resultado


def delta_compensa(delta: DeltaEstado, *, economia_minima_percentual: float = 10.0) -> bool:
    """Retorna True quando o delta é menor que o snapshot pela margem exigida."""
    if delta.vazio or delta.bytes_estado <= 0:
        return False
    return delta.economia_percentual >= max(0.0, float(economia_minima_percentual or 0.0))
