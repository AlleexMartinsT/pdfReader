# PDF Reader / Leitor de Relatórios

## English

Desktop application for reconciling sales reports from two business flows:

- `EH`: automated cashier workflow with Zweb plus Caixa/Azulzinha payment reports.
- `MVA`: PDF-based workflow with `Minhas Notas` reconciliation.

### Current Scope

- Imports commission PDFs and organizes seller results.
- Runs `Caixa > EH` without manual import for the core Zweb reports.
- Fetches EH data from:
  - `Zweb > Documentos > Relatórios > Pedidos importados`
  - `Zweb > Financeiro > Relatórios > Fechamento de caixa`
  - `Zweb > Fiscal > NFC-e`
- Uses Caixa/Azulzinha payment reports for EH reconciliation:
  - The EH loading window now shows a real-time debug log for Gmail token capture and portal automation.
  - Existing local PIX CSV/XLSX/PDF and card PDF/XLSX files are detected by content.
  - If the Caixa PIX export arrives as XLSX, the app converts it to CSV automatically and continues the flow.
  - If PIX or card files are missing, the app attempts to download them from the Caixa/Azulzinha portal.
  - The Caixa/Azulzinha browser now starts minimized and off-screen so the login screen stays less visible during automation.
  - Auto-downloaded EH PIX/card reports named with `_auto` are deleted after they are parsed, so temporary reports do not stay in the workspace.
  - If the portal asks for a token, the app reads the latest code sent by `no-reply@fiserv.com` to the configured Gmail account.
  - Gmail OAuth client credentials can also be loaded from a local `gmail_oauth_client.json` file in the app root.
  - If Caixa/Azulzinha is unavailable and PIX is confirmed through `Financeiro > Movimentações` in Zweb, the report marks that fallback explicitly.
  - If Caixa rejects the token as invalid, the app discards the code it just used and waits longer for a newer email before retrying.
  - Temporary Azulzinha export debug files are cleaned automatically after the flow finishes.
- Runs `Caixa > MVA` with imported PDFs and `Minhas Notas` checks.
- MVA closing screens now mirror the EH sectioned `Fechamento de Caixa` structure in the app and in A4 printing, including the same reconciliation sections and observations block.
- When MVA uses the newer Clipp closing file, the app now also attempts to auto-download missing Caixa/Azulzinha PIX and card reports with the local MVA credentials before reconciling payments.
- Shows the EH `Fechamento de Caixa` with total-sales/pending text plus organized tables for PIX/card missing bank transactions and bank-only transactions without CF/NF.
- Prints sectioned cashier reports through a printer selection dialog.
- Keeps the `Dinheiro` row in value-correlation tables under the `Caixa` column, since it comes from the cashier closing instead of bank-payment exports.
- Uses a more compact A4 print layout so long report titles fit on one line more reliably.
- Normalizes mojibake and broken PT-BR accents in dialogs, reports, and printed output using `ftfy` plus internal fallbacks.
- Auto-updates from GitHub releases using the ZIP asset.

### Development

```powershell
python -m venv .venv64
.\.venv64\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```

### Build

```powershell
.\.venv64\Scripts\python.exe .\build.py
```

## Português

Aplicativo desktop para conciliar relatórios de venda em dois fluxos:

- `EH`: fluxo automático de caixa com Zweb e relatórios de pagamento Caixa/Azulzinha.
- `MVA`: fluxo por PDFs com conferência no `Minhas Notas`.

### Escopo Atual

- Importa PDFs de comissão e organiza os resultados por vendedor.
- Executa `Caixa > EH` sem importação manual dos relatórios principais do Zweb.
- Busca os dados da `EH` em:
  - `Zweb > Documentos > Relatórios > Pedidos importados`
  - `Zweb > Financeiro > Relatórios > Fechamento de caixa`
  - `Zweb > Fiscal > NFC-e`
- Usa relatórios de pagamento Caixa/Azulzinha na conciliação da EH:
  - Arquivos locais de PIX em CSV/XLSX/PDF e cartões em PDF/XLSX são identificados pelo conteúdo.
  - Se o PIX da Caixa vier em XLSX, o app converte automaticamente para CSV e continua o fluxo.
  - Se os arquivos de PIX ou cartões estiverem ausentes, o app tenta baixá-los no portal Caixa/Azulzinha.
  - Se o portal solicitar token, o app lê automaticamente o código mais recente enviado por `no-reply@fiserv.com` no Gmail configurado.
  - Se a Caixa/Azulzinha não estiver disponível e o PIX for confirmado por `Financeiro > Movimentações` no Zweb, o relatório sinaliza esse fallback de forma explícita.
- Executa `Caixa > MVA` com PDFs importados e conferência no `Minhas Notas`.
- O fechamento da MVA agora espelha a estrutura seccionada do `Fechamento de Caixa` da EH no aplicativo e na impressão A4, incluindo as mesmas seções de conciliação e observações.
- Quando a MVA usa o fechamento novo do Clipp, o app agora também tenta baixar automaticamente os relatórios PIX e cartões da Caixa/Azulzinha com as credenciais locais da MVA antes de conciliar os pagamentos.
- Exibe o `Fechamento de Caixa` da EH com texto de total de vendas/pendências e tabelas organizadas para divergências de PIX e cartão.
- Imprime relatórios seccionados de caixa pela tela de seleção de impressora.
- Normaliza mojibake e acentuação quebrada em diálogos, relatórios e impressão com `ftfy` e fallback interno.
- Mantem a linha `Dinheiro` da correlacao de valores na coluna `Caixa`, porque ela vem do fechamento do caixa e nao do pagamento bancario.
- Usa uma diagramacao A4 mais compacta para que titulos longos caibam melhor em uma unica linha.
- Atualiza automaticamente pelo ZIP publicado nas releases do GitHub.
  - Se a Caixa rejeitar o token como invalido, o app descarta o codigo usado e aguarda mais tempo por um novo e-mail antes de tentar de novo.
  - Os arquivos temporarios de debug da exportacao da Azulzinha sao limpos automaticamente ao final do fluxo.
  - Relatorios automaticos da EH com nome `_auto` sao excluidos depois do parse, para nao poluir o workspace.
  - O navegador da Caixa/Azulzinha agora inicia minimizado e fora da area visivel para deixar o login menos evidente durante a automacao.
### EH Debug

- EH loading now includes a real-time debug panel for Gmail token lookup and portal automation steps.
- A tela de carregamento da EH agora inclui um painel de debug em tempo real para a busca do token no Gmail e para as etapas da automação do portal.

## Manual Cashier Notes / Notas do Caixa Manual

- `Caixa > EH` keeps the Zweb reports automatic.
- For EH, the only manual part is the local machine files placed in the app root:
  - PIX: `CSV`, `XLSX` or `PDF`
  - Cards: `PDF` or `XLSX`
- `Caixa > MVA` accepts the legacy coupon report or the newer `clipp_exportado.htm.pdf` closing file.
- When the Clipp closing file is used, MVA also loads local Caixa PIX/card reports from the root folder and builds bank reconciliation tables closer to the EH flow.
- A4 cashier prints keep centered tables with line wrapping for long cells.

- `Caixa > EH` mantém os relatórios do Zweb automáticos.
- Na EH, a única parte manual são os arquivos locais da maquininha na pasta raiz:
  - PIX: `CSV`, `XLSX` ou `PDF`
  - Cartões: `PDF` ou `XLSX`
- `Caixa > MVA` aceita o relatÃ³rio antigo de Cupons ou o novo fechamento `clipp_exportado.htm.pdf`.
- Quando o fechamento do Clipp Ã© usado, a MVA tambÃ©m carrega os relatÃ³rios locais de PIX/cartÃµes da Caixa na pasta raiz e monta a conciliaÃ§Ã£o bancÃ¡ria no mesmo padrÃ£o da EH.
- A impressÃ£o A4 do caixa mantÃ©m tabelas centralizadas e quebra de linha para cÃ©lulas longas.
