"""Regras puras de geração e ordenação da agenda classificatória.

Este módulo não acessa Flask, sessão, Socket.IO ou PostgreSQL. Ele recebe
estruturas simples e devolve rodadas/slots previsíveis, facilitando testes.
"""

import json

def gerar_rodadas_round_robin(equipes):
    """Gera rodadas reais todos-contra-todos pelo método do círculo.

    Rodada aqui NÃO é a ordem física da partida. Rodada é o bloco lógico em que
    uma equipe joga no máximo uma vez. Exemplo com 6 equipes: 5 rodadas, cada
    rodada com 3 jogos. Com número ímpar, uma equipe folga e a folga gira.
    """
    times = list(equipes or [])
    if len(times) < 2:
        return []
    if len(times) % 2 == 1:
        times.append(None)
    n = len(times)
    rodadas = []
    for rodada_idx in range(n - 1):
        jogos = []
        folga = None
        for i in range(n // 2):
            t1 = times[i]
            t2 = times[n - 1 - i]
            if t1 is None or t2 is None:
                folga = t1 or t2
                continue
            if rodada_idx % 2 == 0:
                jogos.append((t1, t2))
            else:
                jogos.append((t2, t1))
        rodadas.append({'numero': rodada_idx + 1, 'jogos': jogos, 'folga': folga})
        times = [times[0]] + [times[-1]] + times[1:-1]
    return rodadas

def numero_rodada_info(rodada_info, padrao=1):
    if isinstance(rodada_info, dict):
        try:
            return int(rodada_info.get('numero') or padrao)
        except (TypeError, ValueError):
            return padrao
    return padrao

def jogos_rodada_info(rodada_info):
    """Retorna somente confrontos válidos (equipe_a, equipe_b).

    Compatibilidade importante:
    - _gerar_rodadas_round_robin() retorna dict com {"numero", "jogos", "folga"};
    - versões antigas/rotas auxiliares podem mandar lista de tuplas;
    - nunca devemos transformar dict em list(dict), porque isso vira
      ["numero", "jogos", "folga"] e causa ValueError no unpack.
    """
    if isinstance(rodada_info, dict):
        jogos_raw = rodada_info.get('jogos') or []
    else:
        jogos_raw = rodada_info or []
    jogos = []
    for jogo in jogos_raw:
        if isinstance(jogo, dict):
            equipe_a = jogo.get('equipe_a') or jogo.get('a') or jogo.get('time_a')
            equipe_b = jogo.get('equipe_b') or jogo.get('b') or jogo.get('time_b')
        elif isinstance(jogo, (list, tuple)) and len(jogo) >= 2:
            equipe_a, equipe_b = (jogo[0], jogo[1])
        else:
            continue
        if equipe_a and equipe_b:
            jogos.append((equipe_a, equipe_b))
    return jogos

def ids_quadras_ativas(quadras):
    ids = []
    for q in quadras or []:
        if q.get('ativa') is False:
            continue
        try:
            ids.append(int(q.get('id')))
        except (TypeError, ValueError):
            pass
    return ids

def normalizar_lista_ids(valores):
    if valores in (None, ''):
        return []
    if isinstance(valores, str):
        try:
            valores = json.loads(valores)
        except Exception:
            valores = [v.strip() for v in valores.split(',')]
    ids = []
    for v in valores or []:
        try:
            n = int(v)
            if n > 0 and n not in ids:
                ids.append(n)
        except (TypeError, ValueError):
            pass
    return ids

def montar_fila_jogos_classificatorios(rodadas_por_grupo, rodizio):
    """Monta uma fila respeitando rodadas reais entre grupos."""
    fila = []
    grupos = sorted(rodadas_por_grupo.keys())
    max_rodadas = max((len(r) for r in rodadas_por_grupo.values()), default=0)
    if rodizio == 'por_grupo_inteiro':
        for grupo in grupos:
            for pos, rodada_info in enumerate(rodadas_por_grupo.get(grupo) or [], start=1):
                rodada_num = numero_rodada_info(rodada_info, pos)
                for equipe_a, equipe_b in jogos_rodada_info(rodada_info):
                    fila.append({'grupo': grupo, 'rodada_grupo': rodada_num, 'equipe_a': equipe_a, 'equipe_b': equipe_b})
        return fila
    for rodada_idx in range(max_rodadas):
        for grupo in grupos:
            rodadas = rodadas_por_grupo.get(grupo) or []
            if rodada_idx >= len(rodadas):
                continue
            rodada_info = rodadas[rodada_idx]
            rodada_num = numero_rodada_info(rodada_info, rodada_idx + 1)
            for equipe_a, equipe_b in jogos_rodada_info(rodada_info):
                fila.append({'grupo': grupo, 'rodada_grupo': rodada_num, 'equipe_a': equipe_a, 'equipe_b': equipe_b})
    return fila

def jogo_respeita_descanso(jogo, historico_slots, descanso_minimo):
    if descanso_minimo <= 0:
        return True
    equipes = {jogo['equipe_a'], jogo['equipe_b']}
    for slot in historico_slots[-descanso_minimo:]:
        if equipes.intersection(slot):
            return False
    return True

def proximo_jogo_sem_conflito(lista_jogos, equipes_slot, equipes_slot_anterior=None):
    """Remove e retorna o primeiro jogo possível sem conflito no slot.

    Primeiro tenta evitar equipes que jogaram no slot anterior. Se não existir
    opção, relaxa essa regra para não travar grupos com poucos times/quadra única
    como o caso da Apolo.
    """
    equipes_slot = set(equipes_slot or set())
    equipes_slot_anterior = set(equipes_slot_anterior or set())
    for idx, jogo in enumerate(lista_jogos or []):
        equipes = {jogo.get('equipe_a'), jogo.get('equipe_b')}
        if equipes.intersection(equipes_slot):
            continue
        if equipes_slot_anterior and equipes.intersection(equipes_slot_anterior):
            continue
        return lista_jogos.pop(idx)
    for idx, jogo in enumerate(lista_jogos or []):
        equipes = {jogo.get('equipe_a'), jogo.get('equipe_b')}
        if equipes.intersection(equipes_slot):
            continue
        return lista_jogos.pop(idx)
    return None

def grupo_com_mais_rodadas_restantes(rodadas_por_grupo, grupos_pool, ultimo_grupo=None):
    candidatos = []
    for grupo in grupos_pool or []:
        restante = len(rodadas_por_grupo.get(grupo) or [])
        if restante <= 0:
            continue
        if ultimo_grupo and grupo == ultimo_grupo and (len(grupos_pool) > 1):
            continue
        candidatos.append((restante, grupo))
    if not candidatos and ultimo_grupo:
        for grupo in grupos_pool or []:
            restante = len(rodadas_por_grupo.get(grupo) or [])
            if restante > 0:
                candidatos.append((restante, grupo))
    if not candidatos:
        return None
    candidatos.sort(key=lambda x: (-x[0], x[1]))
    return candidatos[0][1]

def gerar_slots_pool_multiquadra(rodadas_por_grupo, grupos_pool, quadras_pool):
    """Gera slots físicos mantendo a rodada lógica correta.

    A quadra/slot serve só para ordenar e distribuir jogos. O campo `rodada_grupo`
    continua sendo a rodada real do todos-contra-todos. Assim, se o Grupo A tem
    6 equipes, a Rodada 1 fica com 3 jogos, mesmo que precise de mais de um slot
    físico para executar todos eles.
    """
    capacidade = max(1, len(quadras_pool or []))
    slots = []
    max_rodadas = max((len(rodadas_por_grupo.get(g) or []) for g in grupos_pool or []), default=0)
    for rodada_idx in range(max_rodadas):
        for grupo in sorted(grupos_pool or []):
            rodadas = rodadas_por_grupo.get(grupo) or []
            if rodada_idx >= len(rodadas):
                continue
            rodada_info = rodadas[rodada_idx]
            rodada_num = numero_rodada_info(rodada_info, rodada_idx + 1)
            jogos = jogos_rodada_info(rodada_info)
            while jogos:
                jogos_slot = []
                equipes_slot = set()
                for qid in quadras_pool[:capacidade]:
                    if not jogos:
                        break
                    equipe_a, equipe_b = jogos.pop(0)
                    if equipe_a in equipes_slot or equipe_b in equipes_slot:
                        jogos.insert(0, (equipe_a, equipe_b))
                        break
                    jogos_slot.append({'grupo': grupo, 'equipe_a': equipe_a, 'equipe_b': equipe_b, 'quadra_id': qid, 'rodada_grupo': rodada_num})
                    equipes_slot.update({equipe_a, equipe_b})
                if jogos_slot:
                    slots.append(jogos_slot)
                else:
                    break
    return slots

def gerar_slots_pool_quadra_unica(rodadas_por_grupo, grupos_pool, quadra_id):
    """Gera slots para uma quadra só sem transformar cada jogo em nova rodada.

    Com uma quadra, os jogos são sequenciais, mas a rodada lógica permanece: a
    Rodada 1 mostra todos os jogos da Rodada 1, depois a Rodada 2, e assim vai.
    """
    slots = []
    max_rodadas = max((len(rodadas_por_grupo.get(g) or []) for g in grupos_pool or []), default=0)
    for rodada_idx in range(max_rodadas):
        for grupo in sorted(grupos_pool or []):
            rodadas = rodadas_por_grupo.get(grupo) or []
            if rodada_idx >= len(rodadas):
                continue
            rodada_info = rodadas[rodada_idx]
            rodada_num = numero_rodada_info(rodada_info, rodada_idx + 1)
            for equipe_a, equipe_b in jogos_rodada_info(rodada_info):
                slots.append([{'grupo': grupo, 'equipe_a': equipe_a, 'equipe_b': equipe_b, 'quadra_id': quadra_id, 'rodada_grupo': rodada_num}])
    return slots
