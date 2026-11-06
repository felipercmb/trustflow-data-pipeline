# Operação, replay e diagnóstico

## Execução inicial

```bash
python3 -m trustflow generate-demo --output data/demo
python3 -m trustflow ingest data/demo
python3 -m trustflow report --as-of 2025-01-16T00:00:00Z --output reports/demo
```

O comando imprime o resultado em JSON no stdout e logs no stderr. Exit codes: **0** para execução aprovada; **1** para erro de execução ou carga parcialmente rejeitada; **2** para argumentos inválidos; **3** quando `check`, `audit` ou `verify-export` executam e encontram uma reprovação. Os argumentos inválidos do `argparse` produzem ajuda em texto.

O horário de observação do relatório é obrigatório para tornar a janela explícita. Para observar outro instante, passe outro `--as-of` com fuso. O gerador se recusa a sobrescrever pastas não vazias.

## Replay idempotente

```bash
python3 -m trustflow ingest data/demo
```

Depois da carga inicial padrão, o resultado agregado deve mostrar `inserted_rows: 0` e `duplicate_rows: 441`. A auditoria ganha novas tentativas, mas eventos, indicadores de negócio e referências aos lotes originais permanecem iguais.

Para retomar sem acrescentar tentativas de arquivos já aceitos:

```bash
python3 -m trustflow ingest data/demo --skip-accepted
```

O resultado mostra `skipped_batches: 15` na amostra inicial. Conteúdo modificado tem outro hash e passa pela validação normal. Conteúdo rejeitado nunca é silenciosamente ignorado. Se precisar continuar depois de um arquivo inválido, acrescente `--continue-on-error`; o comando registra o resumo, processa os seguintes e retorna 1. A unidade de commit continua sendo o arquivo.

## Auditoria e reconstrução

```bash
python3 -m trustflow audit --data-dir data/local
python3 -m trustflow rebuild --data-dir data/local --destination data/rebuilt
python3 -m trustflow audit --data-dir data/rebuilt
```

A auditoria verifica todos os brutos referenciados, inclusive os rejeitados, além de integridade SQLite, chaves estrangeiras, fatos, ciclos e contabilidade. Lotes corretamente rejeitados não são, por si, corrupção. `running_batches` é informado separadamente; investigue processos interrompidos antes de reconstruir.

A reconstrução recusa origem com achados de integridade ou tentativas `running`, verifica cada bruto novamente, seleciona apenas lotes aceitos e publica um destino novo após comparar o fingerprint completo dos fatos. Nunca varra todo o bronze para reconstruir indiscriminadamente: ele contém também lotes rejeitados.

Somente fatos de negócio são reconstruídos. Novos IDs de lote e horários de aceitação são gerados, e tentativas rejeitadas ou repetidas não são copiadas para o destino. Para preservar exatamente o histórico de `known_at`, use backup completo do banco e bronze. O comando mantém uma leitura consistente da origem e pode atrasar escritores durante a operação.

## Políticas de qualidade

```bash
python3 -m trustflow check --as-of 2025-01-16T00:00:00Z --policy policies/demo-quality.toml
python3 -m trustflow check --as-of 2025-01-16T00:00:00Z --policy policies/illustrative-service.toml
```

O primeiro comando aprova o cenário estrutural padrão. O segundo deve reprovar com exit code 3 e mostrar o valor observado e o limite para cada regra. `null` em SLA não é aprovação: produz `no_eligible_population`. Uma política `operational` não pode avaliar dados `synthetic`, e campos desconhecidos são rejeitados para evitar erros de digitação silenciosos.

Regras disponíveis: mínimo de solicitações, máximo de órfãos, máximo de lotes rejeitados/em andamento, mínimo de elegíveis a SLA, mínimo de percentual no prazo, máximo de pendências com prazo atingido e máximo de idade da pendência mais antiga. Limites de contagem/idade são inteiros não negativos; percentual fica entre 0 e 100.

`check` avalia o relatório, não faz auditoria de hashes. Execute `audit` separadamente quando precisar verificar a integridade física. Regras sobre lotes usam o estado **atual** da auditoria, mesmo com `--known-at`; a saída identifica esse escopo.

## Publicação de relatórios

```bash
python3 -m trustflow report --as-of 2025-01-16T00:00:00Z --output reports/snapshot-01
python3 -m trustflow verify-export reports/snapshot-01
```

JSON, CSVs e manifesto são preparados juntos antes da publicação. Uma repetição idêntica é aceita; uma saída diferente exige outra pasta. O manifesto contém hashes SHA-256 dos quatro arquivos de dados. Ele detecta alterações acidentais, não autentica o autor: alguém com permissão para substituir arquivos e manifesto pode recalcular hashes. Não é uma assinatura digital.

## Quarentena

Lotes inválidos não carregam nenhuma linha em `events`. O recibo `quarantine/<batch_id>.json` contém o código do erro, mensagem e localização do original. Para corrigir a origem, produza **outro arquivo** e reenvie; não edite a cópia bronze.

| Código | Ação |
|---|---|
| `INVALID_JSON`, `INVALID_ENCODING`, `DUPLICATE_JSON_KEY` | Corrigir serialização da origem |
| `SCHEMA_FIELDS`, `SCHEMA_VERSION`, `INVALID_ENUM` | Ajustar mapeamento ao contrato v1 |
| `INVALID_TIMESTAMP`, `INVALID_ID`, `INVALID_FAILURE_CODE` | Corrigir o campo indicado, mantendo informação de origem |
| `EVENT_ID_CONFLICT` | Investigar reutilização de identificador; não gerar UUID novo para esconder uma correção |
| `LIFECYCLE_CONFLICT`, `DIMENSION_CONFLICT`, `TEMPORAL_ORDER` | Verificar semântica e sequência do fluxo |
| `RAW_INTEGRITY` | Investigar alteração do armazenamento; restaurar de uma cópia verificada |
| `CONFIG_MISMATCH` | Usar o SLA e tipo corretos ou um warehouse separado |
| `BATCH_TOO_LARGE`, `ROW_COUNT` | Particionar a origem respeitando os limites |
| `IO_ERROR`, `DATABASE_ERROR` | Conferir permissões, espaço e escritores concorrentes |
| `SOURCE_AUDIT_FAILED`, `REBUILD_MISMATCH` | Corrigir achados de origem; não publicar reconstrução incompleta |
| `UNSAFE_DESTINATION`, `OUTPUT_EXISTS` | Escolher pasta nova sem sobrescrever evidência anterior |
| `INVALID_POLICY`, `POLICY_DATASET_MISMATCH` | Conferir versão, regras, tipos e natureza do dataset |
| `INVALID_MANIFEST` | Conferir se o conjunto de relatórios está completo e preservado |

Erros antes da criação do lote, como arquivo ausente, excesso de bytes ou divergência de configuração, não geram recibo de quarentena. Erros de acesso/disco podem impedir o diagnóstico persistido. Preserve stderr do agendador e não suponha que toda falha tenha sido registrada no banco.

## Diagnóstico SQL

Com o comando opcional `sqlite3` instalado:

```sql
SELECT status, COUNT(*) AS attempts,
       SUM(inserted_rows) AS inserted, SUM(duplicate_rows) AS duplicates
FROM batches GROUP BY status;

SELECT batch_id, error_code, error_message, raw_key
FROM batches WHERE status = 'rejected' ORDER BY started_at;

SELECT workflow_id, terminal_type, terminal_at
FROM workflow_lifecycle WHERE status = 'awaiting_request';

SELECT e.event_id, e.workflow_id, b.raw_sha256, b.raw_key
FROM events e JOIN batches b ON b.batch_id = e.source_batch_id
LIMIT 20;
```

Não exclua silenciosamente tentativas `running` após uma interrupção. Confirme que não há processo ativo, preserve a evidência e execute novamente os arquivos. O replay cria outra tentativa; o registro interrompido continua disponível para investigação.

## Backup e publicação

O diretório `data/` contém bruto e warehouse; `reports/` contém saídas. Ambos são ignorados pelo Git. Para um backup consistente com processo ativo, use a API `sqlite3.Connection.backup()` e copie o bronze imutável. Uma cópia direta do banco enquanto há escrita pode ser inconsistente.

Os únicos dados versionados neste projeto são exemplos explicitamente sintéticos. Não publique bancos locais, credenciais ou dados reais. Um gerador e pipeline offline são entregues; não há acesso a sistemas produtivos nem job recorrente habilitado.
