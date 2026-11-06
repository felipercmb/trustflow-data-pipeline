# Evolução 0.2: três ciclos de revisão

## Ciclo 1 — carga e retomada

**Problema observado:** a versão 0.1 fazia um lookup por evento e uma consulta por fluxo afetado. Repetir uma pasta inteira também criava uma tentativa de auditoria para cada arquivo, mesmo quando o objetivo era somente retomar uma carga interrompida.

**Mudança:** staging temporário, detecção de conflitos por SQL, inserção dos fatos distintos e validação do conjunto afetado. `--skip-accepted` permite retomada pelo hash do conteúdo; `--continue-on-error` permite processar os outros arquivos sem esconder a falha global.

**Verificação:** contratos e invariantes anteriores preservados; teste com 1.000 eventos verifica que o número de SELECTs não cresce por registro; duas entregas concorrentes iguais geram um fato; duas conflitantes deixam uma aceita e uma rejeitada. JSON com aninhamento excessivo passa a gerar rejeição registrada, em vez de deixar uma tentativa em andamento.

## Ciclo 2 — rastreabilidade e recuperação

**Problema observado:** o bronze era preservado, mas não havia comando para confrontá-lo com fatos e contadores. Reconstruir exigia seleção manual de arquivos, com risco de tentar carregar brutos rejeitados. Relatórios históricos também podiam incorporar dados que chegaram depois da data de interesse.

**Mudança:** `audit` confere integridade e linhagem; `rebuild` audita, seleciona apenas conteúdo aceito, reconstrói em área temporária e compara os fatos antes de publicar. `known_at` filtra o lote da primeira aceitação, preservado em replays idênticos.

**Verificação:** testes de bruto ausente/alterado, alteração de fatos no banco, endereço de bruto inválido, contadores, falha de disco durante recuperação, reconstrução com conteúdo rejeitado e snapshots antes/depois de eventos atrasados. Bancos da versão 0.1 continuam legíveis sem modificação pelo relatório.

## Ciclo 3 — operação e consumo

**Problema observado:** coortes diárias não mostravam onde estavam as pendências mais antigas. Regras de aprovação eram implícitas. JSON e CSV eram gravados separadamente, permitindo uma saída incompleta em caso de interrupção.

**Mudança:** segmentos por canal/documento, backlog em faixas de idade, políticas TOML e exit code específico para reprovação. Exportações passam a ser conjuntos completos com manifesto; saídas anteriores não são sobrescritas. `verify-export` verifica cada arquivo.

**Verificação:** fronteiras das faixas, população de SLA vazia, política de tipo errado, regras desconhecidas, reprovação deliberada, falha durante exportação e adulteração de arquivos. As métricas de referência são regeneradas e comparadas automaticamente com o gerador sintético.

## Medição local e limites

`python3 -m benchmarks.run --workflows 10000 --trials 3` mede ingestão e relatório em três warehouses novos. Geração, rede, auditoria e exportação ficam fora do cronômetro. O resultado inclui versões, arquitetura, tempos e fingerprints. A igualdade dos fingerprints verifica os fatos reproduzidos, não implica igualdade de metadados de execução.

O benchmark serve para detectar regressões locais. Não é um teste de carga distribuído, não estima demanda real e não autoriza alegações de economia ou capacidade de produção. A versão continua sendo batch local com SQLite, sem integração com serviços externos, sem dados pessoais e sem processamento criptográfico de documentos.

## Compatibilidade

- Contrato de eventos permanece na versão 1; não há migração de fatos.
- Pacote passa a 0.2.0; JSON de relatório anuncia `report_version: 2`.
- Relatórios acrescentam contexto, segmentos e backlog. As métricas existentes mantêm suas definições.
- Exportação agora recusa uma pasta com conteúdo diferente. Use um novo diretório por snapshot.
- Reconstrução preserva fatos de negócio, mas renova horários de ingestão. Para preservar o histórico completo, faça backup.
- Hashes são verificações de integridade, sem autenticidade criptográfica de um produtor externo.
