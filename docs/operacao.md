# Operação, replay e diagnóstico

## Execução inicial

```bash
python3 -m trustflow generate-demo --output data/demo
python3 -m trustflow ingest data/demo
python3 -m trustflow report --as-of 2025-01-16T00:00:00Z --output reports/demo
```

O comando imprime o resultado em JSON no stdout e logs de lotes aceitos no stderr. Falhas esperadas produzem JSON com `status`, `code` e `message`, além de exit code 1. Sucesso retorna 0. Erros de argumentos do `argparse` retornam 2 e ajuda em texto.

O horário de observação do relatório é obrigatório para tornar a janela explícita. Para observar outro instante, passe outro `--as-of` com fuso. O gerador se recusa a sobrescrever pastas não vazias.

## Replay idempotente

```bash
python3 -m trustflow ingest data/demo
```

Depois da carga inicial padrão, o resultado agregado deve mostrar `inserted_rows: 0` e `duplicate_rows: 441`. A auditoria ganha novas tentativas, mas eventos, indicadores de negócio e referências aos lotes originais permanecem iguais.

Para reconstruir em um destino separado usando arquivos bronze já preservados:

```python
from pathlib import Path
from trustflow.pipeline import ingest

for raw in sorted(Path("data/local/bronze").glob("*/*.ndjson")):
    ingest(raw, Path("data/rebuilt"))
```

Esse exemplo supõe que todos os brutos selecionados eram válidos, como na demonstração. Um arquivo originalmente rejeitado será novamente rejeitado e deve ser selecionado ou excluído com base no histórico `batches`, nunca alterado no bronze. Depois compare os indicadores de negócio dos dois warehouses no mesmo `as_of`. Identificadores e contadores de tentativas são metadados de execução e podem diferir.

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
