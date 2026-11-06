# Dicionário de métricas

## Grão, janela e população

Um evento tem grão `event_id`. Um fluxo tem grão `workflow_id`. A coorte reúne fluxos pela data UTC de `requested_at`, com filtros opcionais de canal e tipo de documento.

O relatório considera apenas eventos com `occurred_at <= as_of` antes de reconstruir os fluxos. O corte é inclusivo. Sem `known_at`, todos os fatos já ingeridos participam, inclusive eventos atrasados. Com `known_at`, o lote de primeira aceitação também deve ter `finished_at <= known_at`.

Exemplo: uma assinatura ocorreu às 10h e só chegou às 12h. Com `as_of=11h` e sem `known_at`, ela aparece após a carga das 12h. Com `as_of=11h` e `known_at=11h`, permanece fora do relatório. Uma retransmissão idêntica não altera o lote original do evento. `finished_at` é registrado pelo relógio da aplicação imediatamente antes do commit; não é timestamp externo de certificação e depende da correção desse relógio.

Os contadores de lotes em `quality.batches_by_status_current` representam o estado atual da ingestão. Não são filtrados por `as_of`, `known_at`, canal ou documento. Portanto, um relatório histórico completo pode ter novos contadores de auditoria, enquanto suas métricas e fingerprint de eventos permanecem iguais. `source_event_count` conta eventos selecionados; `event_snapshot_sha256` identifica pares ordenados de `event_id` e hash canônico, sem incluir metadados de tentativa.

## Segmentos e idade das pendências

`segments` agrupa a mesma população por canal e tipo de documento, com contagens de estados, elegibilidade de SLA e tempo médio de assinaturas concluídas. Suas contagens somam os totais globais. Não confunda tempo médio de conclusões com idade das pendências.

`backlog` contém apenas fluxos com solicitação e sem terminal observado. A idade é `as_of - requested_at`; as faixas são disjuntas, com limite inferior inclusivo:

| Faixa | Idade |
|---|---|
| `within_sla` | menor que 1 × SLA |
| `one_to_four_sla` | de 1 × SLA até menos de 4 × SLA |
| `four_to_twenty_four_sla` | de 4 × SLA até menos de 24 × SLA |
| `over_twenty_four_sla` | maior ou igual a 24 × SLA |

Faixas vazias não são exportadas. A soma de `pending_count` das faixas coincide com a quantidade total de pendências. `oldest_age_seconds` permite priorizar a fila mais antiga sem revelar identidade ou conteúdo de documentos.

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
