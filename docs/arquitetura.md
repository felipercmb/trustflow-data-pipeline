# Arquitetura e decisões

## Camadas

**Bronze — origem preservada.** Cada arquivo é lido com limite de tamanho, recebe SHA-256 e é gravado por conteúdo em `bronze/<prefixo>/<hash>.ndjson`. Um arquivo temporário é sincronizado e publicado com criação atômica por hard link. Se o destino já existir, seu hash é conferido. Uma corrupção gera erro; não é sobrescrita. O sistema de arquivos precisa suportar hard links, como os sistemas locais usuais de macOS, Linux e Windows/NTFS.

**Silver — fatos validados.** `events` guarda eventos únicos, tempo UTC, microssegundos inteiros, dimensões e referência ao lote original. `batches` registra tentativas, hash do bruto, contagens, estado e diagnóstico. `settings` fixa tipo do dataset, versão e SLA. A view `workflow_lifecycle` apresenta o estado atualmente conhecido de cada fluxo.

**Gold — semântica analítica.** SQL com CTEs e funções de janela produz coortes, SLA, tempo médio e p95. A consulta do relatório abre uma transação de leitura para que todos os resultados venham do mesmo snapshot do banco. JSON e CSV são exportações locais; a fonte autoritativa dos fatos é SQLite mais bronze.

## Transação e recuperação

1. Ler o arquivo dentro do limite de tamanho.
2. Abrir o warehouse e conferir configuração.
3. Preservar os bytes originais.
4. Registrar a tentativa como `running` e confirmar esse registro.
5. Validar contrato; iniciar `BEGIN IMMEDIATE` para serializar escritores.
6. Comparar hashes, inserir fatos novos e validar os fluxos afetados, inclusive fatos anteriores.
7. Confirmar eventos e `accepted` na mesma transação, ou fazer rollback e registrar `rejected`.

O processamento de uma pasta ordena os nomes dos arquivos e faz **uma transação por arquivo**. Um erro para a pasta na primeira falha; lotes anteriores permanecem aceitos. Reexecutar a pasta é seguro para os lotes idênticos.

Uma interrupção entre as etapas pode deixar bronze sem referência ou tentativa `running`. SQLite reverte uma transação incompleta; o replay resolve os fatos sem duplicá-los. Não há recuperação automática de um status `running`, nem transação distribuída entre banco, sistema de arquivos, recibo de quarentena e exportações. Falta de disco pode impedir a gravação do diagnóstico. Esses limites são explícitos.

## Por que Python e SQLite?

O volume demonstrativo cabe em um processo. A biblioteca padrão permite clonar, testar e executar sem credenciais, APIs ou infraestrutura externa. SQLite fornece transações, índices, restrições e SQL analítico com baixo custo de reprodução.

O objetivo é demonstrar correção, operação e clareza das decisões. Não foi medido throughput, latência de produção, custo em nuvem ou economia financeira. Não há garantia de alta disponibilidade ou processamento distribuído.

## Limites relevantes

- Um escritor por vez; conexão aguarda até 30 segundos pelo lock. Use um único agendador local.
- Até 10 MiB / 50 mil linhas por lote, carregado em memória. A validação do ciclo consulta cada fluxo afetado, com índice; volumes maiores exigiriam staging e validação SQL em conjunto.
- Bronze e auditoria crescem sem política automática de retenção.
- Uma solicitação e um terminal por fluxo. Assinatura com múltiplos participantes, reabertura e revisões exigem outro modelo.
- Sem watermark de completude, SLA de atualização da fonte ou garantia de recebimento de todos os eventos. Órfãos são contados, não descartados.
- Datas futuras são aceitas pelo contrato e excluídas de relatórios anteriores. Uma regra de tolerância ao desvio de relógio dependeria do produtor real.
- Relatórios históricos podem mudar após eventos atrasados; não há bitemporalidade nem histórico de revisões de fatos.
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
