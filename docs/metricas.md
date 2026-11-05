# Dicionário de métricas

## Grão, janela e população

Um evento tem grão `event_id`. Um fluxo tem grão `workflow_id`. A coorte reúne fluxos pela data UTC de `requested_at`, com filtro opcional de canal.

O relatório considera apenas eventos com `occurred_at <= as_of` antes de reconstruir os fluxos. O corte é inclusivo. Dados com tempo futuro não vazam para a observação passada. Todos os dados **já ingeridos** podem participar, inclusive eventos atrasados. Portanto, o relatório representa tempo de evento, não uma reprodução do que era conhecido no instante original.

Os contadores de lotes em `quality.batches_by_status_current` representam o estado atual da ingestão. Eles não são filtrados por `as_of` nem por canal; os demais indicadores são filtrados. Reexecutar um lote aumenta o histórico de auditoria, sem aumentar eventos ou indicadores de negócio.

## Estados

| Métrica | Cálculo |
|---|---|
| `requested_count` | Fluxos com solicitação observada |
| `signed_count` | Solicitados cujo terminal observado é `signed` |
| `failed_count` | Solicitados cujo terminal observado é `failed` |
| `canceled_count` | Solicitados cujo terminal observado é `canceled` |
| `expired_count` | Solicitados cujo terminal observado é `expired` |
| `pending_count` | Solicitados sem evento terminal observado |
| `overdue_pending_count` | Pendentes cuja idade é maior ou igual ao SLA |
| `workflows_awaiting_request` | Fluxos com terminal observado, sem solicitação observada |

Invariante: `requested = signed + failed + canceled + expired + pending`.

`failure_events_by_code` conta eventos `failed`, inclusive órfãos. Por isso sua soma pode diferir de `failed_count` enquanto existirem fluxos `awaiting_request`. Ela não mistura falha de negócio com rejeição técnica de lote.

## SLA demonstrativo

O SLA é configurado por warehouse, em segundos, e vale **tempo corrido**. Não considera expediente, feriados, região ou tipo de contrato. O valor padrão de 3.600 segundos é uma escolha do exemplo.

- **Elegível (`sla_due_count`):** solicitação com idade `as_of - requested_at >= SLA`.
- **Cumpriu (`sla_met_count`):** elegível, com terminal `signed` e duração `<= SLA`.
- **Percentual (`sla_met_pct`):** `100 × sla_met_count / sla_due_count`, arredondado a duas casas; `null` quando não houver elegíveis.

Falhos, cancelados, expirados e pendentes elegíveis continuam no denominador. A escolha evita medir SLA somente nos fluxos bem-sucedidos, mas uma política de negócio real poderia excluir cancelamentos ou aplicar prazos diferentes. Isso deve ser decidido antes de usar os indicadores operacionalmente.

Uma assinatura concluída em 30 minutos, observada aos 40 minutos, ainda não participa da taxa cujo SLA é de uma hora. Ela entra junto com toda a população da mesma idade quando o prazo amadurece. No exato limite de uma hora, a solicitação passa a ser elegível; assinatura no mesmo instante cumpre o SLA.

## Duração e p95

`avg_signed_seconds` e `p95_signed_seconds` consideram somente fluxos assinados observados, em cada coorte. Não descrevem o tempo de falhas nem o tempo de espera dos pendentes. São `null` quando a coorte não tem assinaturas concluídas.

`duração = (terminal_us - requested_us) / 1.000.000`.

O p95 usa **nearest rank**: ordene as N durações e selecione a posição `ceil(0,95 × N)`, começando em 1. Para duas durações, 1.800 e 7.200 segundos, a média é 4.500 e o p95 é 7.200. Não se calcula um p95 global pela média dos p95 diários.

As taxas totais usam a soma dos numeradores dividida pela soma dos denominadores, sem média simples de percentuais diários.

## Como interpretar a demonstração

No cenário de semente 42, existem 240 solicitações elegíveis e 41 assinadas no prazo de uma hora: 17,08%. O número é deliberadamente resultado de cenários de teste, não evidência de desempenho da Soluti ou de outro serviço. A utilidade é mostrar uma conta reproduzível, com denominador, janela e limitações explícitos.
