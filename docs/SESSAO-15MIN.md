# Sessão de 15 minutos — O dado mudou. Ainda dá para voltar a aplicação?

Preparada em 09/11/2026. Material técnico disponível; nenhuma palestra agendada ou aceita.

Público: liderança técnica, engenharia, QA e gestão de projetos. Objetivo: sair com três critérios para decidir uma mudança e explicar ao negócio o limite da recuperação.

## Resumo para organização
Demonstração com dados fictícios de duas falhas comuns: repetir uma entrega após o commit e tentar voltar uma aplicação depois de alterar o contrato dos dados. O exercício conecta identidade do evento, compatibilidade e reconciliação a uma decisão de aceite. Python e SQLite, sem serviços pagos ou acesso a cliente. Desenvolvimento com assistência de IA e testes automatizados; revisão externa ainda pendente.

## Preparação
Python 3.10+, terminal na raiz deste repositório. Executar testes antes da sessão. Usar os JSONs em `evidence/` caso a demonstração ao vivo falhe; informar que são de uma execução anterior. Não apresentar arquivo como nova execução. Não usar dados de empresa.

## Roteiro cronometrado

| Tempo | Atividade | Evidência ou pergunta |
|---|---|---|
| 0:00–1:30 | Apresentar a decisão | O que acontece com pedidos já gravados quando voltamos o código? |
| 1:30–3:00 | Delimitar o cenário | Origem, consumidor, identidade e versão; simulação local |
| 3:00–6:00 | Executar `python lab.py demo` | Falha após commit, antes do ACK; repetição sem novo efeito local |
| 6:00–10:00 | Executar `python change_lab.py demo` | Dois pedidos preservados, snapshot sem escrita posterior, contrato antigo bloqueado |
| 10:00–12:00 | Traduzir para aceite | Compatibilidade, reconciliação e autoridade de decisão |
| 12:00–14:00 | Discutir um contraponto | E se o efeito fosse externo ao banco? O exemplo não o protege |
| 14:00–15:00 | Fechar e convidar à revisão | Qual falha faltou? Registrar uma reprodução, sem prometer SLA |

## Três critérios que o público leva
1. Provar qual efeito já aconteceu antes de repetir uma operação.
2. Demonstrar que a versão anterior ainda entende os dados durante a janela de retorno.
3. Explicitar o que um snapshot deixaria de fora e quem aprova a recuperação/reconciliação.

## Perguntas e respostas
- **É exactly-once?** Não. O transporte repete; o primeiro módulo protege apenas o efeito local na transação testada.
- **Backup resolve?** Precisa ser restaurável e reconciliado com as escritas posteriores; o exercício apenas inspeciona a diferença.
- **Posso copiar para produção?** Este material não é implementação operacional. Contratos, retenção, infraestrutura, efeitos externos e testes próprios precisam de desenho e validação.
- **Qual ganho foi medido?** Nenhum ganho de cliente, custo, prazo, RTO ou RPO foi medido.

Minibio: Jeferson Resende atua como Tech Lead, conectando liderança técnica, gestão de projetos e arquitetura de soluções. Trabalha com integrações corporativas e decisões de entrega e operação. Apresentação pessoal; não representa empregadores ou clientes.
