CORREÇÃO DA DEMORA EM SALVAR FINALIZAÇÃO

Arquivos alterados:
- app.py
- banco.py

Mudanças:
- criação/alteração da tabela de destaques movida para a inicialização;
- nenhuma DDL é executada no clique "Salvar finalização";
- lock_timeout de 2,5 s e statement_timeout de 10 s;
- mensagem controlada caso o banco esteja momentaneamente bloqueado;
- log FINALIZACAO com tempos de conexão, lock, destaque, update e commit.
