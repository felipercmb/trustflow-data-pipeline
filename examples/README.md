# Exemplos sintéticos

`demo-report/` contém uma saída real do código sobre **dados gerados**, com semente 42 e 240 fluxos. Não é uma extração de qualquer empresa.

Reproduza em um warehouse novo:

```bash
python3 -m trustflow generate-demo --output data/repro-input --seed 42 --count 240
python3 -m trustflow ingest data/repro-input --data-dir data/repro-store
python3 -m trustflow report --data-dir data/repro-store --as-of 2025-01-16T00:00:00Z --output reports/repro
```

O `report.json` deve coincidir com a referência em uma carga inicial. Se houver replays, `quality.batches_by_status_current` terá mais tentativas; as métricas de negócio permanecem iguais. Os CSVs representam coortes, segmentos e backlog. `manifest.json` confere os hashes dos quatro arquivos de dados; execute `python3 -m trustflow verify-export examples/demo-report`.

As exportações são imutáveis. Se repetir o exemplo depois de acrescentar novas tentativas ou mudar os parâmetros, escolha outro diretório de saída. Horários de ingestão e IDs de lote não são incluídos no fingerprint de eventos, permitindo comparar os fatos entre reconstruções.
