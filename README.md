# PDF Reader / Leitor de Relatórios

## English

Desktop application for reconciling sales reports from two business flows:

- `EH`: automatic Zweb-based cashier workflow
- `MVA`: PDF-based workflow with `Minhas Notas` reconciliation

### Current scope

- Import commission PDFs and organize seller results.
- Run `Caixa > EH` without manual PDF import for the core EH flow.
- Fetch EH data from:
  - `Zweb > Documentos > Relatórios > Pedidos importados`
  - `Zweb > Financeiro > Relatórios > Fechamento de caixa`
  - `Zweb > Fiscal > NFC-e`
- Reconcile temporary local bank files placed beside the app:
  - PIX report: CSV preferred, PDF supported
  - Card report: PDF
- Run `Caixa > MVA` with imported PDFs and `Minhas Notas` checks.
- Export summary and reconciliation views to PDF.
- Auto-update from GitHub releases using the ZIP asset.

### EH workflow

For `EH`, the app now:

1. Loads imported orders from Zweb.
2. Loads cashier closing from Zweb.
3. Loads NFC-e fiscal status from Zweb.
4. Matches imported orders with closing entries.
5. Ignores cancelled fiscal documents when applicable.
6. Reads local PIX and card reports from the app root only when they match the requested day.
7. Builds a single `Conciliação Bancária` view for unresolved PIX/card differences.

### MVA workflow

For `MVA`, the app still uses:

1. Imported DAV/order PDFs
2. Imported coupon summary PDF
3. Optional imported budget PDF
4. `Minhas Notas` reconciliation for missing NF-e values

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

Output:

- `dist/Relatorio de Clientes/`
- `dist/RelatorioClientes-<version>.zip`

### Local temporary files

These files are intentionally local-only and should stay beside the executable only while reconciling a day:

- `relatório pix.pdf`
- `relatório cartoes.pdf`
- `Relatorio_de_Vendas_Pix_*.csv`

### Notes

- This project is for internal use.
- Credentials used by the automation can be embedded in the app for production flows.
- The updater expects a GitHub release ZIP asset.

---

## Português

Aplicativo desktop para conciliar relatórios de venda em dois fluxos:

- `EH`: fluxo automático de caixa via Zweb
- `MVA`: fluxo por PDFs com conferência no `Minhas Notas`

### Escopo atual

- Importa PDFs de comissão e organiza os resultados por vendedor.
- Executa `Caixa > EH` sem importação manual dos PDFs principais.
- Busca os dados da `EH` em:
  - `Zweb > Documentos > Relatórios > Pedidos importados`
  - `Zweb > Financeiro > Relatórios > Fechamento de caixa`
  - `Zweb > Fiscal > NFC-e`
- Concilia arquivos bancários temporários colocados ao lado do app:
  - relatório de PIX: preferência por CSV, com suporte a PDF
  - relatório de cartões: PDF
- Executa `Caixa > MVA` com PDFs importados e conferência no `Minhas Notas`.
- Exporta resumos e telas de conciliação em PDF.
- Atualiza automaticamente pelo ZIP publicado nas releases do GitHub.

### Fluxo EH

Na `EH`, o aplicativo agora:

1. carrega `Pedidos importados` pelo Zweb;
2. carrega `Fechamento de caixa` pelo Zweb;
3. carrega o status fiscal de `NFC-e` pelo Zweb;
4. relaciona pedidos importados com os cupons do fechamento;
5. desconsidera documentos fiscais cancelados quando aplicável;
6. lê os relatórios locais de PIX/cartão somente se forem do dia solicitado;
7. monta uma visão única de `Conciliação Bancária` para divergências restantes.

### Fluxo MVA

Na `MVA`, o aplicativo continua usando:

1. PDFs importados de DAVs/pedidos;
2. PDF importado do resumo de cupons;
3. PDF opcional de orçamentos;
4. conferência com `Minhas Notas` para valores de NF-e faltantes.

### Desenvolvimento

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

Saída:

- `dist/Relatorio de Clientes/`
- `dist/RelatorioClientes-<version>.zip`

### Arquivos temporários locais

Esses arquivos devem ficar apenas localmente, ao lado do executável, quando forem usados na conciliação do dia:

- `relatório pix.pdf`
- `relatório cartoes.pdf`
- `Relatorio_de_Vendas_Pix_*.csv`

### Observações

- Projeto para uso interno.
- As credenciais usadas pela automação podem ficar embutidas no app nos fluxos de produção.
- O atualizador espera um asset ZIP na release do GitHub.
