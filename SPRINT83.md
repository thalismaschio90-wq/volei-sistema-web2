# Fase 4 — Sprint 83: autoria e trilha de auditoria

## Objetivo

Registrar, nos novos eventos persistidos, metadados mínimos de quem executou a ação e de qual requisição ela veio, sem criar novas colunas e sem armazenar IP ou User-Agent brutos.

## Implementação

- `core/audit_context.py`: contexto isolado por requisição usando `ContextVar`.
- `app.py`: captura usuário, nome, perfil, endpoint, método, caminho e fingerprints anônimos.
- `banco.py`: enriquece o JSON `detalhes` dos eventos com a seção `auditoria`.
- `services/replay_partida.py`: lê autoria nova e mantém compatibilidade com eventos antigos.
- `templates/admin_replay_partida.html`: exibe autor, perfil, origem e identificador curto da requisição.

## Dados registrados

```json
{
  "auditoria": {
    "request_id": "...",
    "usuario": "...",
    "nome": "...",
    "perfil": "apontador",
    "endpoint": "apontadores.registrar_ponto",
    "metodo": "POST",
    "caminho": "/...",
    "origem": "web",
    "ip_fingerprint": "...",
    "dispositivo_fingerprint": "..."
  }
}
```

IP e User-Agent não são armazenados em texto. Somente fingerprints SHA-256 truncados são persistidos.

## Compatibilidade

- nenhuma tabela foi alterada;
- eventos legados continuam sendo exibidos;
- quando não existe contexto HTTP, o evento recebe `origem=sistema`;
- os contratos públicos das funções existentes foram preservados.

## Validação

- 335 testes aprovados;
- compilação completa dos módulos Python;
- testes de privacidade dos fingerprints;
- testes de autoria no replay;
- testes de fallback para eventos internos.
