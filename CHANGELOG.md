# Changelog

## 0.2.0 — 2026-11-06

- Carga com staging SQL, validação de ciclos em conjunto e índices de acesso.
- Retomada por conteúdo aceito e opção de continuar após rejeições com exit code de falha.
- Auditoria de hashes, integridade SQLite, linhagem, contadores e ciclos.
- Reconstrução em novo destino com seleção de lotes aceitos e comparação de fatos.
- Relatórios por tempo de evento e tempo de primeira aceitação (`known_at`).
- Segmentação por canal/tipo de documento e backlog por idade em relação ao SLA.
- Políticas TOML com limites explícitos, distinção entre dados sintéticos/operacionais e exit code 3 para reprovação.
- Exportações completas e imutáveis com manifesto e comando de verificação.
- Testes de concorrência, falha durante publicação, integridade e compatibilidade com a versão anterior.
- Benchmark sintético local reproduzível, sem alegação de desempenho de produção.

## 0.1.0 — 2026-11-05

- Pipeline inicial Python/SQL, bronze por hash, contrato v1, cargas atômicas e idempotentes.
- Coortes, SLA e duração de assinaturas com dados explicitamente sintéticos.
- Testes offline, documentação e GitHub Actions.
