# Fase 2 — Sprint 42: laboratório de carga e sincronização

Esta sprint adiciona um laboratório externo. Ele não roda dentro do Web Service e não
consome CPU da aplicação quando não é chamado.

## Segurança

O modo padrão é somente leitura:

```env
VTP_LOAD_ALLOW_WRITES=0
```

O registro automático de pontos somente é habilitado quando `VTP_LOAD_ALLOW_WRITES=1`
e existe uma sessão de apontador ou token de homologação. Nunca execute escrita em uma
partida real.

## Instalação na máquina de teste

```bash
python -m venv .venv-load
.venv-load/bin/pip install -r requirements.txt -r requirements-loadtest.txt
```

No Windows:

```powershell
py -m venv .venv-load
.venv-load\Scripts\pip install -r requirements.txt -r requirements-loadtest.txt
```

## Primeiro teste: 30 espectadores, somente leitura

```env
VTP_LOAD_BASE_URL=https://SEU-SERVICO-HOMOLOGACAO.onrender.com
VTP_LOAD_COMPETICAO=Competição de Homologação
VTP_LOAD_PARTIDA_ID=123
VTP_LOAD_PUBLIC_CODE=ABC123
VTP_LOAD_VIEWERS=30
VTP_LOAD_DURATION_SECONDS=120
VTP_LOAD_ALLOW_WRITES=0
VTP_LOAD_SOCKET_ENABLED=1
```

Execute:

```bash
python scripts/executar_teste_carga.py
```

## Teste completo com apontador

Use uma partida descartável e forneça o cookie de sessão do apontador:

```env
VTP_LOAD_ALLOW_WRITES=1
VTP_LOAD_SESSION_COOKIE=session=...
VTP_LOAD_POINT_INTERVAL_SECONDS=2
```

O laboratório alterna pontos simples entre A e B. Substituição, tempo, sanção e troca
de set devem continuar sendo executados manualmente durante o teste, enquanto o
laboratório mede as páginas públicas e a entrega Socket.IO.

## Sequência recomendada de homologação

1. Redis + 1 worker, 30 visualizadores, 2 minutos.
2. Redis + 1 worker, 100 visualizadores, 5 minutos.
3. Redis + 2 workers, 30 visualizadores, 5 minutos.
4. Redis + 2 workers, 100 visualizadores, partida completa.
5. Teste manual de substituição, tempo, sanção e troca de set durante a carga.

## Critérios iniciais

- `/readyz`: sempre HTTP 200.
- Registro de ponto: P95 abaixo de 500 ms.
- Páginas públicas: P95 abaixo de 1.000 ms.
- Nenhum receptor com versões Socket.IO regressivas.
- Nenhum ponto duplicado.
- Nenhum reinício inesperado do Web Service.

Os relatórios são gravados em `load_reports/` nos formatos JSON e Markdown.
