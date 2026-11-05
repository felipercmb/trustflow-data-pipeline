# Contexto e fontes

## Relação com o setor

O site institucional da [Soluti](https://soluti.com.br/) apresenta certificados digitais, Bird ID e a IntelliSign, descrita como solução de assinaturas e gestão de documentos. Essa informação orienta o domínio do projeto: **observabilidade analítica de fluxos de assinatura**.

Uma operação desse tipo pode precisar acompanhar solicitações pendentes, prazos e falhas por canal. É uma hipótese de uso para demonstrar engenharia de dados. Não foi validada como demanda interna da Soluti. Não usamos logotipo, dados, APIs privadas, documentação interna nem alegamos parceria.

O projeto também não afirma reproduzir a arquitetura da empresa ou todos os requisitos de uma vaga. Seu foco é apresentar decisões técnicas verificáveis em um domínio pertinente.

## Por que dados sintéticos?

Em 5 de novembro de 2026, foram consultadas a [página de dados abertos do ITI](https://www.gov.br/iti/pt-br/acesso-a-informacao/dados-abertos) e a referência ao [portal de dados abertos](https://dadosabertos.iti.gov.br/). O acesso ao portal retornou HTTP 403. O [ITI em Números](https://numeros.iti.gov.br/) não forneceu conteúdo tabular legível na ferramenta consultada. **Nenhum dataset desses portais foi baixado, validado ou incorporado.** Nenhum bloqueio foi contornado.

Estatísticas agregadas públicas também não representariam diretamente os tempos individuais de fluxos de assinatura necessários à demonstração. Optou-se por um gerador explícito de cenários, sem preencher lacunas com números atribuídos a fontes reais.

## Proveniência dos exemplos

- Origem: [`trustflow/demo.py`](../trustflow/demo.py).
- Parâmetros: `seed=42`, `count=240`.
- Janela das solicitações: 1 a 14 de janeiro de 2025, UTC.
- Eventos terminais: até três horas após a solicitação; podem cair no dia seguinte.
- Marcação obrigatória: `dataset_kind: synthetic` em cada evento e relatório.
- Identificadores: UUIDs determinísticos gerados a partir da semente e do índice; não vêm de pessoas ou sistemas reais.
- Categorias e probabilidades: escolhas didáticas. Não são amostragem, projeção, benchmark ou evidência sobre uma empresa.
- SHA-256 dos arquivos e lote de origem: preservados na execução para rastrear exatamente os bytes processados.

As datas dos eventos são fixas para permitir reprodução. Não representam atividade de 2025 observada pelo autor.

## Referências técnicas

- [SQLite: transações](https://www.sqlite.org/lang_transaction.html).
- [SQLite: funções de janela](https://www.sqlite.org/windowfunctions.html).
- [Python: sqlite3](https://docs.python.org/3/library/sqlite3.html).
- [GitHub Actions para Python](https://docs.github.com/actions/guides/building-and-testing-python).
