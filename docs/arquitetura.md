# Arquitetura e decisões

## Camadas

**Bronze — origem preservada.** Cada arquivo é lido com limite de tamanho, recebe SHA-256 e é gravado por conteúdo em `bronze/<prefixo>/<hash>.ndjson`. Um arquivo temporário é sincronizado e publicado com criação atômica por hard link. Se o destino já existir, seu hash é conferido. Uma corrupção gera erro; não é sobrescrita. O sistema de arquivos precisa suportar hard links, como os sistemas locais usuais de macOS, Linux e Windows/NTFS.

**Silver — fatos validados.** `events` guarda eventos únicos, tempo UTC, microssegundos inteiros, dimensões e referência ao lote original. `batches` registra tentativas, hash do bruto, contagens, estado e diagnóstico. `settings` fixa tipo do dataset, versão e SLA. A view `workflow_lifecycle` apresenta o estado atualmente conhecido de cada fluxo.

**Gold — semântica analítica.** Uma transação de leitura cria tabelas temporárias em memória com os eventos elegíveis por `as_of`, `known_at`, canal e tipo de documento. Todas as consultas usam esse mesmo snapshot. SQL com CTEs e funções de janela produz coortes, SLA, tempo médio, p95, segmentos e backlog. O banco de origem é aberto em modo somente leitura; as tabelas temporárias não o alteram.

## Transação e recuperação

1. Ler o arquivo dentro do limite de tamanho.
2. Abrir o warehouse e conferir configuração.
3. Preservar os bytes originais.
4. Registrar a tentativa como `running` e confirmar esse registro.
5. Validar contrato; iniciar `BEGIN IMMEDIATE` para serializar escritores.
6. Carregar uma tabela temporária de staging com `executemany`, detectar conflitos por joins/agregações, inserir os fatos distintos e validar o conjunto dos fluxos afetados, inclusive fatos anteriores.
7. Confirmar eventos e `accepted` na mesma transação, ou fazer rollback e registrar `rejected`.

O processamento de uma pasta ordena os nomes dos arquivos e faz **uma transação por arquivo**. Por padrão, um erro para a pasta na primeira falha; `--continue-on-error` processa os arquivos seguintes e ainda retorna exit code 1. `--skip-accepted` compara hashes já aceitos sem criar outra tentativa; bruto alterado no armazenamento continua gerando erro de integridade. Reexecutar sem esse flag preserva a auditoria de cada replay.

`audit` verifica o banco, chaves estrangeiras, endereços/hashes do bruto, consistência do ciclo, contabilidade dos lotes e correspondência dos fatos com seus arquivos de origem. Um lote rejeitado pode estar corretamente preservado; isso não reprova a auditoria de integridade, mas pode reprovar uma política operacional.

`rebuild` mantém um snapshot da origem, audita, seleciona apenas arquivos vinculados a lotes aceitos, elimina repetições do mesmo bruto e carrega um diretório temporário. Só publica o destino depois que o fingerprint dos fatos coincide com a origem. O comando não altera a origem e recusa destinos existentes ou dentro dela. Como reconstrói por ingestão, gera novos horários de aceitação; não substitui um backup completo do histórico.

`export_report` grava JSON, três CSVs e manifesto em um diretório temporário irmão do destino. O diretório completo é então renomeado. Conteúdo diferente em destino existente é recusado; exportação idêntica é idempotente. Esse padrão evita conjuntos parcialmente publicados, mas não é uma garantia contra perda de energia no sistema de arquivos nem uma transação distribuída.

Uma interrupção entre as etapas pode deixar bronze sem referência ou tentativa `running`. SQLite reverte uma transação incompleta; o replay resolve os fatos sem duplicá-los. Não há recuperação automática de um status `running`, nem transação distribuída entre banco, sistema de arquivos, recibo de quarentena e exportações. Falta de disco pode impedir a gravação do diagnóstico. Esses limites são explícitos.

## Por que Python e SQLite?

O volume demonstrativo cabe em um processo. A biblioteca padrão permite clonar, testar e executar sem credenciais, APIs ou infraestrutura externa. SQLite fornece transações, índices, restrições e SQL analítico com baixo custo de reprodução.

O objetivo é demonstrar correção, operação e clareza das decisões. Há um benchmark local reproduzível; não foi medido throughput de produção, custo em nuvem ou economia financeira. Não há garantia de alta disponibilidade ou processamento distribuído.

## Limites relevantes

- Um escritor por vez; conexão aguarda até 30 segundos pelo lock. Use um único agendador local.
- Até 10 MiB / 50 mil linhas por lote, carregado em memória. A carga usa staging e validação SQL em conjunto; o relatório materializa a população selecionada em memória. Volumes maiores exigem outro planejamento de armazenamento e execução.
- Bronze e auditoria crescem sem política automática de retenção.
- Uma solicitação e um terminal por fluxo. Assinatura com múltiplos participantes, reabertura e revisões exigem outro modelo.
- Sem watermark de completude, SLA de atualização da fonte ou garantia de recebimento de todos os eventos. Órfãos são contados, não descartados.
- Datas futuras são aceitas pelo contrato e excluídas de relatórios anteriores. Uma regra de tolerância ao desvio de relógio dependeria do produtor real.
- Sem `known_at`, relatórios históricos podem mudar após eventos atrasados. Com ele, fatos são filtrados pelo horário de primeira aceitação registrado no lote. Isso depende de relógio confiável e preservação do histórico; não é um sistema bitemporal completo nem controla revisões de fatos.
- Auditorias e reconstruções leem toda a origem selecionada e podem atrasar escritores; execute recuperação em janela operacional. Tentativas `running` bloqueiam a reconstrução e exigem investigação.
- O hash detecta alteração dos bytes armazenados, mas não autentica um produtor nem substitui controle de acesso.
- Não há autenticação, criptografia de disco ou conector remoto. Os controles de dados reais precisariam ser definidos antes dessa integração.

## Evolucao

Extensões propostas, **não implementadas**:

| Necessidade futura | Caminho possível | Critério antes de adotar |
|---|---|---|
| Origem operacional | Adaptador autorizado com identidade do produtor | Contrato e acesso validados; política de minimização |
| Mais volume | Bronze em object storage, staging e carga por lote em warehouse | Benchmark e custo medidos |
| Agendamento | Orquestrador com dependências, retry e alertas | Operação de recuperação e destino persistente definidos |
| Correções | Eventos de revisão versionados / histórico bitemporal | Regra sobre o que pode mudar e como auditar |
| BI | Modelo semântico com as mesmas populações e denominadores | Reconciliação entre SQL e painel |
