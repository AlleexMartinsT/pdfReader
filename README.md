# PDF Reader / Leitor de Relatórios

## English

Desktop application for reconciling sales reports from two business flows:

- `EH`: automated cashier workflow with Zweb plus Caixa/Azulzinha payment reports.
- `MVA`: PDF-based workflow with `Minhas Notas` reconciliation.

### Current Scope

- Imports commission PDFs and organizes seller results.
- Runs `Caixa > EH` without manual import for the core Zweb reports.
- The Zweb `Pedidos importados` and `Fechamento de caixa` HTML reports are now saved in the current execution folder so the user can inspect them after the run.
- Fetches EH data from:
  - `Zweb > Documentos > Relatórios > Pedidos importados`
  - `Zweb > Financeiro > Relatórios > Fechamento de caixa`
  - `Zweb > Fiscal > NFC-e`
- Uses Caixa/Azulzinha payment reports for EH reconciliation:
  - The EH loading window now shows a concise real-time debug log for Gmail token capture and portal automation, without listing recent Gmail messages one by one, and it includes the code selected for validation.
  - Existing local PIX CSV/XLSX/PDF and card PDF/XLSX files are detected by content in the current execution folder.
  - If Caixa leaves a valid PIX/card download with a `.crdownload` suffix, the app now normalizes it and still uses the report.
  - If the Caixa PIX export arrives as XLSX, the app converts it to CSV automatically and continues the flow.
  - If PIX or card files are missing, the app attempts to download them from the Caixa/Azulzinha portal into the current execution folder.
  - If Caixa times out and returns to the login page while opening the PIX/card sales area, the app now reauthenticates and retries the report instead of waiting on a dead sales tab.
  - PIX export now waits longer for the results/export button, waits longer for the final file, and retries the report once when Caixa is slow to generate it.
  - If Caixa opens its generic `_error.html` page during automatic PIX/card export, the app now detects it, reloads the sales area, and retries the report once.
  - Automatic Caixa steps now include extra settle time after tab switches and date filters, and they wait for loading placeholders to disappear before continuing.
  - The Caixa/Azulzinha automation now follows an explicit portal-state flow (`login`, `device`, `token`, `sales`), so real screen transitions are treated as progress instead of false login failures.
  - The portal-state classifier now distinguishes the `/Login` sub-screens by visible content, so device and token pages are no longer mistaken for the CNPJ/password login form or for authenticated `Home`.
  - The automation no longer treats `/MinhasVendas` or `/Home` in the URL alone as proof that the sales area is ready; it now requires the visible sales UI.
  - Each Caixa/Azulzinha automation run now uses a fresh temporary browser profile to reduce failures caused by stale session state.
  - The Caixa/Azulzinha browser now opens in a visible window during automation so the user can inspect the portal flow when needed.
- Auto-downloaded EH PIX/card reports named with `_auto` are preserved after parsing so the user can open them, while leftover partial `.crdownload` files are cleaned.
- Caixa/Azulzinha raw downloads now land in a temporary company-isolated folder and are only then persisted as `..._eh_auto` or `..._mva_auto`, preventing EH/MVA files with the same original name from colliding.
- Local Caixa/Azulzinha auto reports are now company-bound during reuse, so EH never reuses `_mva_auto` files and MVA never reuses `_eh_auto` files from a previous automation run.
  - If the portal asks for a token, the app reads the latest code sent by `no-reply@fiserv.com` to the configured Gmail account.
  - After requesting the Caixa token by email, the app now waits an extra 15 seconds only before the initial Gmail lookup so the new message has time to arrive.
  - The flow now only enters the token step when a real token or delivery UI is visible, avoiding false jumps out of the login screen.
  - Gmail token lookup now queries recent `no-reply@fiserv.com` messages directly and scans a wider Fiserv-only window so unrelated inbox traffic does not hide the latest code.
  - The local `body_email.txt` debug file now records the recent Fiserv emails inspected for the current token attempt, including message id, extracted token, app decision, snippet, and parsed body.
  - Gmail OAuth client credentials can also be loaded from a local `gmail_oauth_client.json` file in the app root.
  - If Caixa/Azulzinha is unavailable and PIX is confirmed through `Financeiro > Movimentações` in Zweb, the report marks that fallback explicitly.
  - If Caixa rejects the token as invalid, the app discards the code it just used, waits 5 seconds, and then requests a newer email before retrying.
  - Token retries now continue from the same invalid-code challenge screen, and the full Caixa automation timeout is now 480 seconds to tolerate slower token and download cycles.
  - When Caixa keeps the token input screen open after an invalid code, the retry now waits for a newer email without re-clicking the email-delivery option, avoiding resend/read-too-early loops.
  - While waiting for a newer token email, the app now rechecks the portal state between Gmail polls and stops the Gmail loop as soon as Caixa is already in the sales area.
  - If Caixa returns to the login screen right after token validation, the app now treats that as a lost session, reauthenticates, and requests a fresh token instead of flagging the code as invalid.
  - Right after token confirmation, the flow now treats `Home` as valid progress and gives the portal up to 15 seconds to reach `Home` or the sales area before treating a transient invalid-token state as a real rejection.
  - When the post-token state is still ambiguous, the flow now validates the session by opening `MinhasVendas` before discarding the token, which is more robust against Azulzinha's variable slowness.
  - Token validation now keeps observing the live portal state until it sees a conclusive outcome such as an actual error message, a return to login, `Home`, or the sales area, instead of deciding from a short analysis window.
  - The token retry flow now inspects the visible challenge UI before re-sending a code, so it does not spam new email tokens while the existing token input screen is still open.
  - The app now also recognizes broader authenticated Azulzinha pages and menu/navigation signals as valid portal progress during login and after token confirmation, before the sales tabs appear.
  - When token validation lands on the authenticated dashboard, the flow now opens `Relatorio de vendas` from the live Azulzinha menu instead of relying only on a direct sales URL.
  - If Azulzinha shows the red portal popup such as `Ops, um erro aconteceu, tente novamente mais tarde` while confirming the token, the automation now treats it as a real portal error and restarts authentication instead of hanging on the token screen.
- EH bank reconciliation now excludes Azulzinha/Caixa payments that are matched to filtered NF-e entries, so the `Pagamentos` column reflects only the NFC-e universe.
- EH cancelled coupons now count toward `Pendências`, `Total Pendências`, and `Faltante` status even when they stay without a dedicated visible evidence row.
- Auto-generated EH reports such as downloaded Caixa/Azulzinha files and saved Zweb HTML exports are now deleted automatically when the app closes, so the workspace does not accumulate `_auto` files between sessions.
- Temporary Azulzinha export debug files are cleaned automatically after the flow finishes.
- The main window now includes a cashier automation controller beside `Cancel`; while enabled, it runs the same-day morning flow at `13:30` and the same-day afternoon flow at `18:10`, and it still sends only the resulting `Fechamento de Caixa` reports straight to the Windows default printer.
- The same automation can now be triggered from the UI with a 5-second test countdown, using the same direct-print flow as the scheduled run without an intermediate preview.
- If the default printer is unavailable during an automatic cashier print, the app now keeps the ready HTML print job as pending, exposes per-company pending-print buttons in the UI, and retries again at `08:00` on the next day while automation remains enabled.
- Automatic cashier printing now reuses the stable A4 `QTextDocument.print_` path, preserving the expected page size instead of shrinking the report during silent printing.
- If MVA source files are still missing at one of those slots, the app now leaves a dedicated pending morning/afternoon button in the UI and still prefetches the MVA Caixa/Azulzinha payment files in advance.
- When a pending MVA morning/afternoon run is resumed from the UI, it now reuses matching local `..._mva_auto` payment files first and only goes back to Azulzinha/Caixa if the current-date payment files are still missing.
- Both the manual `Caixa` flow and the scheduled/test automation now reuse matching local `..._eh_auto` / `..._mva_auto` payment files first, so they do not attempt a fresh Azulzinha/Caixa login when both payment files for that date are already present.
- MVA reconciliation now also consults `Minhas Notas` model `65` to detect canceled coupons and bring them into the pending totals/report when they are absent from the local closing report.
- EH and MVA bank reconciliation now expose canceled coupons in a dedicated `Cupons Cancelados` section instead of mixing them into `CF sem Transação Bancária`.
- Canceled coupons now leave the EH/MVA payment-matching pool before value-only bank reconciliation, so same-value active coupons keep priority and the canceled CF goes to `Cupons Cancelados`.
- EH partial/daily closing generation no longer crashes when a canceled coupon is promoted into the dedicated `Cupons Cancelados` section during bank reconciliation.
- Runs `Caixa > MVA` with imported PDFs and `Minhas Notas` checks.
- MVA closing screens now mirror the EH sectioned `Fechamento de Caixa` structure in the app and in A4 printing, including the same reconciliation sections and observations block.
- When MVA uses the newer Clipp closing file, the app now also attempts to auto-download missing Caixa/Azulzinha PIX and card reports with the local MVA credentials before reconciling payments.
- If the MVA `Fechamento de Caixa` PDF is actually a Clipp `Relatório de Vendas` or another unsupported layout, the app now warns that the file will be treated only as a local report and will not trigger automatic Azulzinha/Caixa payment downloads.
- Manual `Caixa` flows now auto-detect the available closing scope: a single morning closing is generated directly, and when a full-day closing is detected the app lets the user choose between the daily report and an afternoon-only report.
- Afternoon-only EH/MVA reports now reuse the same closing-window logic as the morning flow, filtering Caixa/Azulzinha payments and the supported cashier data down to the afternoon window instead of keeping the whole day.
- Closing-report titles now append `(M)` or `(T)` after `MVA` / `Eletrônica Horizonte` whenever the generated scope is morning or afternoon, while daily reports keep the base title.
- Same-day Caixa/Azulzinha card downloads now switch to the `Hoje` tab instead of `Histórico de vendas`, because the historical calendar only releases dates up to the previous day.
- The Caixa/Azulzinha PIX export flow now confirms the real `Gerar arquivo` step and keeps monitoring the export until the report is actually captured, instead of bailing out on a short quiet window.
- Bank reconciliation reports keep only the visible PIX/card sections, and the `Pendências` counter is recalculated from those displayed rows so the summary matches the visible evidence.
- Shows the EH `Fechamento de Caixa` with total-sales/pending text plus organized tables for PIX/card missing bank transactions and bank-only transactions without CF/NF.
- Prints sectioned cashier reports through a printer selection dialog.
- Keeps printed A4 cashier tables on a shared full-width layout so sections stay visually consistent.
- Preserves bank-only card rows as `Origem / Detalhe / Valor`, so printed card values no longer shift into the detail column.
- Keeps the `Dinheiro` row in value-correlation tables under the `Caixa` column, since it comes from the cashier closing instead of bank-payment exports.
- Uses smaller A4/report-export titles and Lexend in printed/exported cashier reports when the bundled font is available.
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
- Os HTMLs de `Pedidos importados` e `Fechamento de caixa` do Zweb agora ficam salvos na pasta atual de execucao para conferencia depois da rotina.
- Busca os dados da `EH` em:
  - `Zweb > Documentos > Relatórios > Pedidos importados`
  - `Zweb > Financeiro > Relatórios > Fechamento de caixa`
  - `Zweb > Fiscal > NFC-e`
- Usa relatórios de pagamento Caixa/Azulzinha na conciliação da EH:
- Arquivos locais de PIX em CSV/XLSX/PDF e cartões em PDF/XLSX são identificados pelo conteúdo na pasta atual de execução.
  - Se a Caixa deixar um download válido de PIX/cartões com sufixo `.crdownload`, o app agora normaliza esse arquivo e ainda usa o relatório.
  - Se o PIX da Caixa vier em XLSX, o app converte automaticamente para CSV e continua o fluxo.
- Se os arquivos de PIX ou cartões estiverem ausentes, o app tenta baixá-los no portal Caixa/Azulzinha para a pasta atual de execução.
  - Se a Caixa expirar e voltar para o login ao abrir a area de vendas de PIX/cartoes, o app agora refaz a autenticacao e tenta o relatorio de novo, em vez de ficar esperando uma aba de vendas morta.
  - A exportacao do PIX agora espera mais tempo pelos resultados e pelo arquivo final, e tenta o relatorio mais uma vez quando a Caixa demora para gerar o arquivo.
  - Se a Caixa abrir a pagina generica `_error.html` durante a exportacao automatica de PIX ou cartoes, o app agora detecta isso, recarrega a area de vendas e tenta o relatorio mais uma vez.
  - A automacao da Caixa agora inclui folgas extras apos troca de abas e filtro de data, e so continua quando os placeholders de carregamento somem da tela.
  - A automacao da Caixa/Azulzinha agora segue um fluxo explicito de estados do portal (`login`, `device`, `token`, `sales`), entao transicoes reais entre telas deixam de ser tratadas como falhas falsas de login.
  - O classificador de estados do portal agora distingue as subtelas dentro de `/Login` pelo conteudo visivel, para que dispositivo e token nao sejam confundidos com o formulario de CNPJ/senha nem com a `Home` autenticada.
  - A automacao nao trata mais `/MinhasVendas` ou `/Home` na URL, sozinhos, como prova de que a area de vendas esta pronta; agora ela exige a UI visivel de vendas.
  - Cada execução da automação da Caixa/Azulzinha agora usa um perfil temporário novo do navegador para reduzir falhas causadas por sessão residual.
  - Se o portal solicitar token, o app lê automaticamente o código mais recente enviado por `no-reply@fiserv.com` no Gmail configurado.
  - Depois de solicitar o token da Caixa por e-mail, o app agora aguarda 15 segundos extras apenas antes da consulta inicial ao Gmail, para dar tempo da nova mensagem chegar.
  - A busca do token no Gmail agora consulta diretamente as mensagens recentes de `no-reply@fiserv.com` e varre uma janela maior só da Fiserv, para que outros e-mails da caixa de entrada não escondam o código mais novo.
  - O arquivo local `body_email.txt` agora registra os e-mails recentes da Fiserv inspecionados na tentativa atual do token, com message id, token extraído, decisão do app, snippet e corpo analisado.
  - Se a Caixa/Azulzinha não estiver disponível e o PIX for confirmado por `Financeiro > Movimentações` no Zweb, o relatório sinaliza esse fallback de forma explícita.
- Executa `Caixa > MVA` com PDFs importados e conferência no `Minhas Notas`.
- O fechamento da MVA agora espelha a estrutura seccionada do `Fechamento de Caixa` da EH no aplicativo e na impressão A4, incluindo as mesmas seções de conciliação e observações.
- Quando a MVA usa o fechamento novo do Clipp, o app agora também tenta baixar automaticamente os relatórios PIX e cartões da Caixa/Azulzinha com as credenciais locais da MVA antes de conciliar os pagamentos.
- Se o PDF informado como `Fechamento de Caixa` da MVA for na verdade um `Relatório de Vendas` do Clipp ou outro layout não suportado, o app agora avisa que esse arquivo será tratado apenas como relatório local e não acionará o download automático dos pagamentos da Azulzinha/Caixa.
- Os fluxos manuais de `Caixa` agora detectam automaticamente o escopo disponível: se existir só o fechamento da manhã, ele gera a manhã direto; se o fechamento do dia inteiro estiver disponível, o app deixa o usuário escolher entre o relatório diário e o relatório só da tarde.
- Os relatórios só da tarde da EH/MVA agora reutilizam a mesma lógica de janela horária do fechamento da manhã, filtrando os pagamentos da Caixa/Azulzinha e os dados de caixa suportados para a segunda janela do dia, em vez de manter o dia inteiro.
- Os títulos do `Fechamento de Caixa` agora recebem `(M)` ou `(T)` depois de `MVA` / `Eletrônica Horizonte` quando o relatório gerado for de manhã ou de tarde; no diário o título base permanece igual.
- No dia corrente, o download de cartões da Caixa/Azulzinha agora troca automaticamente para a aba `Hoje`, porque o calendário de `Histórico de vendas` só libera datas até ontem.
- O fluxo de exportação PIX da Caixa/Azulzinha agora confirma o passo real de `Gerar arquivo` e continua monitorando a exportação até capturar o relatório de fato, em vez de encerrar por uma janela curta sem eventos.
- Os relatórios de conciliação bancária agora mantêm apenas as seções visíveis de PIX/cartão, e o contador de `Pendências` passa a ser recalculado por essas linhas exibidas para bater com a evidência visível.
- Exibe o `Fechamento de Caixa` da EH com texto de total de vendas/pendências e tabelas organizadas para divergências de PIX e cartão.
- Imprime relatórios seccionados de caixa pela tela de seleção de impressora.
- Normaliza mojibake e acentuação quebrada em diálogos, relatórios e impressão com `ftfy` e fallback interno.
- Mantém as tabelas do caixa na impressão A4 com largura total padronizada, para todas as seções seguirem o mesmo padrão visual.
- Preserva as linhas bancárias de cartão no formato `Origem / Detalhe / Valor`, para o valor impresso não migrar para a coluna errada.
- Mantem a linha `Dinheiro` da correlacao de valores na coluna `Caixa`, porque ela vem do fechamento do caixa e nao do pagamento bancario.
- Usa títulos menores e a fonte Lexend nos relatórios impressos/exportados quando a fonte embutida estiver disponível.
- Atualiza automaticamente pelo ZIP publicado nas releases do GitHub.
  - Se a Caixa rejeitar o token como invalido, o app descarta o codigo usado, aguarda 5 segundos e entao tenta buscar um e-mail mais novo antes de validar de novo.
  - As novas tentativas de token agora continuam da propria tela de codigo invalido, e o timeout total da automacao da Caixa subiu para 480 segundos para acomodar ciclos mais lentos de token e download.
  - Quando a Caixa mantem a tela de digitacao do token aberta apos um codigo invalido, a nova tentativa agora espera um e-mail mais novo sem clicar de novo na opcao de envio por e-mail, evitando loops de reenvio e leitura precoce.
  - Enquanto aguarda um novo e-mail de token, o app agora revalida o estado do portal entre as consultas ao Gmail e encerra o loop do Gmail assim que a Caixa ja estiver na area de vendas.
  - Se a Caixa voltar para a tela de login logo apos validar o token, o app agora trata isso como perda de sessao, refaz a autenticacao e pede um token novo em vez de marcar o codigo como invalido.
  - Logo apos confirmar o token, o fluxo agora trata `Home` como progresso valido e da ao portal ate 15 segundos para avancar para `Home` ou para a area de vendas antes de tratar um estado transitorio de token invalido como rejeicao de fato.
  - Quando o estado logo apos o token ainda fica ambiguo, o fluxo agora valida a sessao abrindo `MinhasVendas` antes de descartar o codigo, o que fica mais robusto contra a lentidao variavel da Azulzinha.
  - A validacao do token agora continua observando o estado real do portal ate aparecer um resultado conclusivo, como uma mensagem real de erro, volta ao login, `Home` ou area de vendas, em vez de decidir por uma janela curta de analise.
  - O retry do token agora inspeciona a interface visivel do desafio antes de reenviar um codigo, para nao spammar novos tokens por e-mail enquanto a mesma tela de digitacao ainda estiver aberta.
  - O app agora tambem reconhece telas internas autenticadas mais amplas da Azulzinha e sinais de menu/navegacao como progresso valido do portal durante o login e depois da confirmacao do token, antes mesmo de as abas de vendas aparecerem.
  - Quando a validacao do token cai no dashboard autenticado, o fluxo agora abre `Relatorio de vendas` pelo menu real da Azulzinha em vez de depender apenas da URL direta da area de vendas.
  - Se a Azulzinha exibir o popup vermelho do portal, como `Ops, um erro aconteceu, tente novamente mais tarde`, ao confirmar o token, a automacao agora trata isso como erro real do portal e reinicia a autenticacao em vez de ficar travada na tela do token.
- A conciliacao bancaria da EH agora exclui da coluna `Pagamentos` os valores da Azulzinha/Caixa que forem identificados como pertencentes a NF-e filtrada, para refletir apenas o universo de NFC-e.
- A conciliacao bancaria da EH e da MVA agora tambem separa cupons cancelados em uma secao dedicada `Cupons Cancelados`, em vez de misturar esses CFs em `CF sem Transacao Bancaria`.
- Os cupons cancelados da EH continuam contando em `Pendências`, `Total Pendências` e no status `Faltante`, agora com exibição dedicada na seção `Cupons Cancelados`.
- A geracao do fechamento parcial/diario da EH nao quebra mais quando um cupom cancelado e promovido para a secao dedicada `Cupons Cancelados` durante a conciliacao bancaria.
- A impressao automatica do caixa agora volta a sair em pagina A4 pelo mesmo caminho estavel do `QTextDocument.print_`, evitando o relatorio minusculo na impressao silenciosa.
  - Relatorios automaticos da EH, como downloads da Caixa/Azulzinha e HTMLs exportados do Zweb, agora sao excluidos automaticamente ao fechar o app, para que o workspace nao acumule arquivos `_auto` entre sessoes.
  - Os arquivos temporarios de debug da exportacao da Azulzinha sao limpos automaticamente ao final do fluxo.
  - Relatorios automaticos da EH com nome `_auto` agora ficam disponiveis depois do parse para o usuario abrir, e apenas sobras parciais como `.crdownload` sao limpas.
  - Os downloads brutos da Caixa/Azulzinha agora primeiro caem em uma pasta temporaria isolada por empresa e so depois sao salvos como `..._eh_auto` ou `..._mva_auto`, evitando colisao entre arquivos da EH e da MVA que venham com o mesmo nome original.
  - Os relatorios automaticos locais da Caixa/Azulzinha agora ficam presos a sua propria empresa no reaproveitamento, entao a EH nunca reutiliza arquivos `_mva_auto` e a MVA nunca reutiliza arquivos `_eh_auto` de uma execucao anterior.
  - O navegador da Caixa/Azulzinha agora abre em janela visivel durante a automacao, para que o usuario possa inspecionar o fluxo do portal quando precisar.
  - O fluxo agora so entra na etapa de token quando a interface real de token ou de entrega estiver visivel, evitando saltos falsos para fora da tela de login.
- A janela principal agora tem um controlador de automacao do caixa ao lado de `Cancelar`; quando ligado, ele roda o fechamento da manha no mesmo dia as `13:30` e o da tarde no mesmo dia as `18:10`, e continua enviando apenas os relatorios de `Fechamento de Caixa` direto para a impressora padrao do Windows.
- O mesmo fluxo de automacao agora pode ser disparado pela interface com um teste de 5 segundos, usando a mesma impressao direta da agenda e sem preview intermediario.
- Se a impressora padrao nao estiver disponivel durante a impressao automatica do caixa, o app agora guarda o HTML pronto como impressao pendente, mostra botoes pendentes por empresa na interface e tenta novamente no dia seguinte as `08:00` enquanto a automacao estiver ligada.
- Se os arquivos-base da MVA ainda estiverem faltando em um desses horarios, o app agora deixa um botao pendente especifico de manha/tarde na interface e mesmo assim adianta o download dos pagamentos da Caixa/Azulzinha da MVA.
- Quando uma pendencia de manha/tarde da MVA e retomada pelo botao da interface, o app agora reaproveita primeiro os pagamentos locais `..._mva_auto` da mesma data e so volta para a Azulzinha/Caixa se esses arquivos ainda estiverem faltando.
- Tanto o fluxo manual de `Caixa` quanto a automacao agendada/de teste agora reaproveitam primeiro os pagamentos locais `..._eh_auto` / `..._mva_auto` da data correspondente, sem tentar novo login na Azulzinha/Caixa quando os dois arquivos de pagamento daquele dia ja estiverem presentes.
- Ao fechar o aplicativo, quando houver relatorios automaticos `_auto` da Azulzinha/Zweb no workspace, o app agora pergunta se o usuario quer manter esses arquivos ou exclui-los antes de encerrar.
- A conciliacao da MVA agora tambem consulta o `Minhas Notas` no modelo `65` para detectar cupons cancelados e traz-los para as pendencias/totais do relatorio quando eles estiverem ausentes do fechamento local.
- Quando um CF cancelado da MVA seria tratado como sobra de pagamento, ele agora vai para a secao `Cupons Cancelados` em vez de aparecer em `CF sem Transacao Bancaria`.
- Cupons cancelados agora saem do pool de conciliacao bancaria da EH/MVA antes do cruzamento por valor, para que um CF ativo de mesmo valor tenha prioridade e o cancelado va para `Cupons Cancelados`.
### EH Debug

- EH loading now includes a concise real-time debug panel for Gmail token lookup and portal automation steps, including the code selected for validation.
- A tela de carregamento da EH agora inclui um painel de debug mais conciso para a busca do token no Gmail e para as etapas da automação do portal, incluindo o codigo escolhido para validacao.

## Manual Cashier Notes / Notas do Caixa Manual

- `Caixa > EH` keeps the Zweb reports automatic.
- For EH, the only manual part is the local machine files placed in the current execution folder:
  - PIX: `CSV`, `XLSX` or `PDF`
  - Cards: `PDF` or `XLSX`
- `Caixa > MVA` accepts the legacy coupon report or the newer `clipp_exportado.htm.pdf` closing file.
- When the Clipp closing file is used, MVA also loads local Caixa PIX/card reports from the current execution folder and builds bank reconciliation tables closer to the EH flow.
- A4 cashier prints keep centered tables with line wrapping for long cells.

- `Caixa > EH` mantém os relatórios do Zweb automáticos.
- Na EH, a única parte manual são os arquivos locais da maquininha na pasta atual de execução:
  - PIX: `CSV`, `XLSX` ou `PDF`
  - Cartões: `PDF` ou `XLSX`
- `Caixa > MVA` aceita o relatÃ³rio antigo de Cupons ou o novo fechamento `clipp_exportado.htm.pdf`.
- Quando o fechamento do Clipp Ã© usado, a MVA tambÃ©m carrega os relatÃ³rios locais de PIX/cartÃµes da Caixa na pasta atual de execução e monta a conciliaÃ§Ã£o bancÃ¡ria no mesmo padrÃ£o da EH.
- A impressÃ£o A4 do caixa mantÃ©m tabelas centralizadas e quebra de linha para cÃ©lulas longas.
