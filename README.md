# PDF Reader (Relatorio de Clientes)

Aplicativo desktop para importar relatorios em PDF e planilhas online, consolidar vendas por vendedor, editar dados locais com seguranca e visualizar indicadores em graficos. Feito para uso interno com dois fluxos de dados (MVA e EH) e exportacoes em PDF.

## Visao geral

O programa le PDFs e planilhas online, identifica a origem (MVA/EH), organiza os dados em tabelas separadas e permite:
- Importar PDFs locais.
- Carregar planilha online.
- Mesclar resultados quando houver as duas fontes.
- Editar a tabela local (com fluxo de salvar em novo PDF).
- Exportar planilhas e feedbacks em PDF.
- Visualizar graficos (MVA, EH, Todos, Vendas totais).

## Funcionalidades principais

- Importacao de PDFs e deteccao de origem (MVA/EH), incluindo tolerancia a nomes similares.
- Planilha online integrada, com tabelas sempre somente leitura.
- Mescla controlada das fontes locais e online.
- Edicao da tabela local com botao dedicado e aviso de alteracoes nao salvas.
- Exportacao de planilhas e feedbacks em PDF.
- Graficos por metricas: Atendidos, Devolucoes, Total Final, Total Vendas.
- Barra(s) de progresso com botao de cancelar.
- Interface responsiva, com colunas ajustaveis e bordas arredondadas.

## Requisitos

- Windows 10/11
- Python 3.13 (recomendado)
- Dependencias em requirements.txt

## Instalacao (dev)

```powershell
python -m venv .venv64
.\.venv64\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```

## Como usar

1) Clique em Importar para selecionar PDFs locais.
2) Use Planilha online para carregar dados remotos.
3) Mesclar Planilhas so fica habilitado quando as duas fontes estao disponiveis.
4) Clique em Editar para liberar a tabela local; ao salvar, um novo PDF e gerado.
5) Exportar permite salvar planilhas ou feedbacks.
6) Graficos alterna para a tela de indicadores.

## Build (Windows)

```powershell
.\.venv64\Scripts\python.exe .\build.py
```

O build gera:
- dist/Relatorio de Clientes/ (onedir)
- dist/RelatorioClientes-<versao>.zip (usado no release)

## Atualizacao

O aplicativo verifica releases do GitHub e baixa o ZIP mais recente. Depois de baixar, extrai em %LOCALAPPDATA%\RelatorioClientes e inicia a versao nova.

## Observacoes

- Tabelas online sao sempre somente leitura.
- A edicao vale apenas para a tabela local.
- O sistema identifica MVA/EH pelo nome do arquivo e por confirmacoes do usuario quando necessario.

## Licenca

Uso interno.
