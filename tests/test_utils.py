import json
import threading
import time
import zipfile
from pathlib import Path

import pytest
import utils
from utils import (
    _active_report_dir,
    parse_number,
    format_number_br,
    _build_card_reports_from_cielo,
    _classify_caixa_document,
    _find_eh_local_payment_reports,
    _filter_zweb_fechamento_to_sales_date,
    get_gmail_oauth_status,
    _is_eh_counter_client,
    _merge_card_machine_report,
    _project_base_dir,
    _mva_caixa_reports_refresh_needs,
    _mva_cielo_pending_card_count,
    _mva_cielo_needed_card_keys,
    _normalize_fiscal_number,
    _refresh_mva_caixa_reports_if_needed,
    _run_gmail_oauth_local_server,
    _wait_for_cielo_downloaded_report,
    _zweb_fechamento_has_sales_outside_date,
    analisar_pdf_fechamento_caixa_mva_clipp,
    aplicar_escopo_relatorio_caixa,
    criar_relatorio_orcamentos_mva_vazio,
    describe_closing_scope,
)

def test_parse_number():
    assert parse_number("R$ 1.500,20") == 1500.20
    assert parse_number("1.500,20") == 1500.20
    assert parse_number("1,500.20") == 1500.20
    assert parse_number("20,50") == 20.50
    assert parse_number("100.00") == 100.00


def test_cleanup_generated_auto_reports_removes_runtime_artifacts(tmp_path, monkeypatch):
    monkeypatch.setattr(utils, "CIELO_DEBUG_LOGS_ENABLED", False)
    should_delete = [
        "body_email.txt",
        "debug_zweb.html",
        "azulzinha_login_wait_state.json",
        "azulzinha_export_debug_pix.html",
        "cielo_mva_debug_26052026_120000.log",
        "cielo_snapshot_26052026_login.html",
        "Relatorio_de_Vendas_Pix_26-05-2026_eh_auto.csv",
        "Historico_Simplificado_de_vendas_26-05-2026_mva_auto.xlsx",
        "Pedidos_importados_26-05-2026_eh_zweb_auto.html",
    ]
    should_keep = [
        "credenciais.txt",
        "gmail_oauth_token.json",
        "relatorio_final_usuario.pdf",
    ]
    for name in should_delete + should_keep:
        (tmp_path / name).write_text("x", encoding="utf-8")
    for dirname in ("azulzinha_browser", "cielo_browser"):
        (tmp_path / dirname / "run_x").mkdir(parents=True)

    utils.cleanup_generated_auto_reports(tmp_path)

    assert all(not (tmp_path / name).exists() for name in should_delete)
    assert all((tmp_path / name).exists() for name in should_keep)
    assert not (tmp_path / "azulzinha_browser").exists()
    assert not (tmp_path / "cielo_browser").exists()


def test_cleanup_generated_auto_reports_preserves_cielo_debug_when_enabled(tmp_path, monkeypatch):
    monkeypatch.setattr(utils, "CIELO_DEBUG_LOGS_ENABLED", True)
    debug_log = tmp_path / "cielo_mva_debug_26052026_120000.log"
    snapshot = tmp_path / "cielo_snapshot_26052026_login.html"
    debug_log.write_text("x", encoding="utf-8")
    snapshot.write_text("x", encoding="utf-8")

    utils.cleanup_generated_auto_reports(tmp_path)

    assert debug_log.exists()
    assert not snapshot.exists()


def test_wait_for_downloaded_report_also_checks_active_report_dir(tmp_path, monkeypatch):
    browser_dir = tmp_path / "browser_downloads"
    active_dir = tmp_path / "app"
    browser_dir.mkdir()
    active_dir.mkdir()
    report = active_dir / "Historico_Simplificado_de_vendas_26-05-2026_eh_auto.csv"
    report.write_text(
        "\n".join(
            [
                "Data da venda;Produto;Status;Valor bruto",
                "26/05/2026 as 08:10;Credito;Aprovada;100,00",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(utils, "_active_report_dir", lambda: str(active_dir))

    found = utils._wait_for_downloaded_report(
        str(browser_dir),
        "26/05/2026",
        "cartoes",
        report.stat().st_mtime - 1,
        timeout=0.0,
    )

    assert found == str(report)


def test_wait_for_downloaded_report_ignores_mtime_skew_in_primary_download_dir(tmp_path):
    browser_dir = tmp_path / "browser_downloads"
    browser_dir.mkdir()
    report = browser_dir / "Relatorio_Simplificado_de_Vendas_Historico_de_Vendas_26-05-2026_0930.xlsx"
    report.write_text(
        "\n".join(
            [
                "Relatório de vendas_Histórico de vendas",
                "Período de Venda:  25/05/2026 à 25/05/2026",
                "Data da venda;Cód. de autorização;Produto;Parcelas;Bandeira;Valor bruto;Status",
                "25/05/2026 08:10:00;ABC123;Crédito à vista;-;Mastercard;100,00;Aprovada",
            ]
        ),
        encoding="utf-8",
    )

    found = utils._wait_for_downloaded_report(
        str(browser_dir),
        "25/05/2026",
        "cartoes",
        report.stat().st_mtime + 600,
        timeout=0.0,
    )

    assert found == str(report)


def test_wait_for_downloaded_report_does_not_accept_pix_as_card(tmp_path):
    browser_dir = tmp_path / "browser_downloads"
    browser_dir.mkdir()
    pix = browser_dir / "Relatorio_de_Vendas_Pix_26-05-2026_0930.xlsx"
    pix.write_text(
        "\n".join(
            [
                "Relatório de Vendas Pix",
                "Período de Venda:  25/05/2026 à 25/05/2026",
                "Data da venda;Cód. de autorização;Valor bruto;Terminal;Status",
                "25/05/2026 às 08:10;SE001;100,00;APT36A49;Aprovada",
            ]
        ),
        encoding="utf-8",
    )

    found = utils._wait_for_downloaded_report(
        str(browser_dir),
        "25/05/2026",
        "cartoes",
        pix.stat().st_mtime - 1,
        timeout=0.0,
    )

    assert found is None


def test_extract_local_report_date_prefers_sales_period_over_emission_date():
    text = "\n".join(
        [
            "Relatorio de Vendas Pix",
            "Emitido em: 26/05/2026 09:33:10",
            "Periodo de Venda: 25/05/2026 ate 25/05/2026",
            "Valor total de vendas finalizadas: R$ 1.234,56",
        ]
    )

    assert utils._extract_local_report_date_br(text) == "25/05/2026"


def test_wait_for_downloaded_report_accepts_pix_xlsx_named_by_download_date(tmp_path):
    pd = pytest.importorskip("pandas")
    browser_dir = tmp_path / "browser_downloads"
    browser_dir.mkdir()
    report = browser_dir / "Relatorio_de_Vendas_Pix_26-05-2026_0933.xlsx"
    pd.DataFrame(
        [
            ["Relatorio de Vendas Pix"],
            ["Emitido em: 26/05/2026 09:33:10"],
            ["Periodo de Venda: 25/05/2026 ate 25/05/2026"],
            ["Valor total de vendas finalizadas: R$ 1.234,56"],
        ]
    ).to_excel(report, index=False, header=False)

    found = utils._wait_for_downloaded_report(str(browser_dir), "25/05/2026", "pix", time.time() - 5, timeout=0.1)

    assert found is not None
    assert Path(found).suffix == ".csv"


def test_caixa_card_report_reads_xls_tabular_text(tmp_path):
    report = tmp_path / "Historico_Simplificado_de_vendas_26-05-2026_eh_auto.xls"
    report.write_text(
        "\n".join(
            [
                "Data da venda;Cód. de autorização;Produto;Status;Valor bruto",
                "26/05/2026 às 08:10;ABC123;Crédito;Aprovada;100,00",
                "26/05/2026 às 08:12;DEF456;Débito;Autorizada;50,00",
            ]
        ),
        encoding="utf-8",
    )

    result = utils._build_card_reports_from_caixa(str(report), "26/05/2026")

    assert result["cartao_credito_caixa"]["total_autorizado"] == 100.0
    assert result["cartao_debito_caixa"]["total_autorizado"] == 50.0
    assert parse_number(15.5) == 15.5
    assert parse_number(None) == 0.0
    assert parse_number(" ") == 0.0


def test_caixa_card_report_deduplicates_same_authorization_across_establishments(tmp_path):
    openpyxl = pytest.importorskip("openpyxl")
    report = tmp_path / "Historico_Simplificado_de_vendas_08-06-2026_mva_auto.xlsx"
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(
        [
            "Data da venda",
            "Cód. de autorização",
            "Comprovante da venda",
            "Produto",
            "Parcelado",
            "Bandeira",
            "Canal",
            "Terminal",
            "Valor bruto",
            "Status",
            "Número do estabelecimento",
        ]
    )
    sheet.append(["08/06/2026 às 08:25", "ABC123", "ABC123", "Crédito", "-", "", "", "", 100.0, "Autorizada", "91111977"])
    sheet.append(["08/06/2026 às 08:25", "ABC123", "ABC123", "Crédito", "-", "", "", "", 100.0, "Autorizada", "91119212"])
    sheet.append(["08/06/2026 às 08:30", "DEF456", "DEF456", "Débito", "-", "", "", "", 50.0, "Autorizada", "91111977"])
    workbook.save(report)

    result = utils._build_card_reports_from_caixa(str(report), "08/06/2026")

    assert result["cartao_credito_caixa"]["quantidade_autorizados"] == 1
    assert result["cartao_credito_caixa"]["total_autorizado"] == 100.0
    assert result["cartao_debito_caixa"]["quantidade_autorizados"] == 1
    assert result["cartao_debito_caixa"]["total_autorizado"] == 50.0


def test_format_number_br():
    assert format_number_br(1500.2) == "1.500,20"
    assert format_number_br(20.5) == "20,50"
    assert format_number_br(0.0) == "0,00"

def test_classify_caixa_document():
    assert _classify_caixa_document("Venda  ") == "Venda"
    assert _classify_caixa_document("Nota Fiscal  Eletrônica") == "Nota Fiscal Eletronica"
    assert _classify_caixa_document("NFC-e") == "NFC-e"

def test_is_eh_counter_client():
    assert _is_eh_counter_client("CLIENTE BALCÃO") is True
    assert _is_eh_counter_client("Cliente Joao") is False

def test_normalize_fiscal_number():
    assert _normalize_fiscal_number("1234") == "000001234"
    assert _normalize_fiscal_number("0001234") == "000001234"


def test_active_report_dir_prefers_frozen_executable_folder(tmp_path, monkeypatch):
    app_dir = tmp_path / "dist" / "Relatorio de Clientes"
    app_dir.mkdir(parents=True)
    exe_path = app_dir / "Relatorio de Clientes.exe"
    exe_path.write_text("", encoding="utf-8")

    monkeypatch.setattr("sys.frozen", True, raising=False)
    monkeypatch.setattr("sys.executable", str(exe_path))

    assert _active_report_dir() == str(app_dir)


def test_project_base_dir_prefers_frozen_executable_folder(tmp_path, monkeypatch):
    app_dir = tmp_path / "dist" / "Relatorio de Clientes"
    app_dir.mkdir(parents=True)
    exe_path = app_dir / "Relatorio de Clientes.exe"
    exe_path.write_text("", encoding="utf-8")

    monkeypatch.setattr("sys.frozen", True, raising=False)
    monkeypatch.setattr("sys.executable", str(exe_path))

    assert _project_base_dir() == str(app_dir)


def test_gmail_oauth_server_honors_pre_cancelled_event():
    cancel_event = threading.Event()
    cancel_event.set()

    with pytest.raises(RuntimeError, match="__cancelled__"):
        _run_gmail_oauth_local_server(object(), cancel_event=cancel_event)


def test_gmail_oauth_status_reports_missing_token(tmp_path, monkeypatch):
    (tmp_path / "gmail_oauth_client.json").write_text(
        json.dumps({"installed": {"client_id": "client", "client_secret": "secret"}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_ID", "")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_SECRET", "")
    monkeypatch.setattr(utils, "_canonical_runtime_dir", lambda: tmp_path)
    monkeypatch.setattr(utils, "_runtime_user_dir", lambda: str(tmp_path))
    monkeypatch.setattr(utils, "_active_report_dir", lambda: str(tmp_path))

    status = get_gmail_oauth_status()

    assert status["needs_auth"] is True
    assert status["status"] == "missing_token"


def test_gmail_oauth_status_finds_client_in_canonical_runtime_dir(tmp_path, monkeypatch):
    canonical_dir = tmp_path / "pdfReader"
    app_dir = canonical_dir / "dist" / "Relatorio de Clientes"
    canonical_dir.mkdir()
    app_dir.mkdir(parents=True)
    (canonical_dir / "gmail_oauth_client.json").write_text(
        json.dumps({"installed": {"client_id": "client", "client_secret": "secret"}}),
        encoding="utf-8",
    )

    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_ID", "")
    monkeypatch.setenv("GMAIL_OAUTH_CLIENT_SECRET", "")
    monkeypatch.setattr(utils, "_canonical_runtime_dir", lambda: canonical_dir)
    monkeypatch.setattr(utils, "_runtime_user_dir", lambda: str(app_dir))
    monkeypatch.setattr(utils, "_active_report_dir", lambda: str(app_dir))

    status = get_gmail_oauth_status()

    assert status["needs_auth"] is True
    assert status["status"] == "missing_token"
    assert status["client_path"] == str(canonical_dir / "gmail_oauth_client.json")
    assert status["token_path"] == str(canonical_dir / "gmail_oauth_token.json")


def test_cielo_gmail_token_uses_recent_fallback_when_timestamp_filter_misses(monkeypatch):
    class FakeResponse:
        status_code = 200

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

        def raise_for_status(self):
            return None

    class FakeSession:
        def get(self, url, params=None, timeout=None):
            if url.endswith("/messages"):
                return FakeResponse({"messages": [{"id": "msg_new"}, {"id": "msg_old"}]})
            msg_id = url.rsplit("/", 1)[-1]
            fmt = (params or {}).get("format")
            if fmt == "metadata":
                internal_date = "100000" if msg_id == "msg_new" else "90000"
                return FakeResponse(
                    {
                        "id": msg_id,
                        "internalDate": internal_date,
                        "payload": {
                            "headers": [
                                {"name": "Subject", "value": "Cielo | Confirmação de e-mail"},
                                {"name": "From", "value": "Cielo <cielo@comunica.cielo.com.br>"},
                            ]
                        },
                    }
                )
            token = "903863" if msg_id == "msg_new" else "751036"
            return FakeResponse(
                {
                    "id": msg_id,
                    "internalDate": "100000" if msg_id == "msg_new" else "90000",
                    "payload": {},
                    "snippet": f"Para sua segurança, use o código de verificação abaixo: {token}",
                }
            )

    monkeypatch.setattr(utils, "_cielo_gmail_session", lambda **_kwargs: FakeSession())
    debug_info = {}

    token = utils._fetch_cielo_token_from_gmail(
        timeout=0.1,
        min_internal_ts=100.5,
        debug_info=debug_info,
    )

    assert token == "903863"
    assert debug_info["candidate_count"] == 0
    assert debug_info["fallback_candidate_count"] == 2
    assert debug_info["selected_lookup_mode"] == "latest_recent"


def test_cielo_gmail_token_does_not_use_stale_fallback(monkeypatch):
    class FakeResponse:
        status_code = 200

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

        def raise_for_status(self):
            return None

    class FakeSession:
        def get(self, url, params=None, timeout=None):
            if url.endswith("/messages"):
                return FakeResponse({"messages": [{"id": "msg_old"}]})
            msg_id = url.rsplit("/", 1)[-1]
            fmt = (params or {}).get("format")
            if fmt == "metadata":
                return FakeResponse(
                    {
                        "id": msg_id,
                        "internalDate": "100000",
                        "payload": {
                            "headers": [
                                {"name": "Subject", "value": "Cielo | Confirmação de e-mail"},
                                {"name": "From", "value": "Cielo <cielo@comunica.cielo.com.br>"},
                            ]
                        },
                    }
                )
            return FakeResponse(
                {
                    "id": msg_id,
                    "internalDate": "100000",
                    "payload": {},
                    "snippet": "Para sua segurança, use o código de verificação abaixo: 903863",
                }
            )

    monkeypatch.setattr(utils, "_cielo_gmail_session", lambda **_kwargs: FakeSession())
    monkeypatch.setattr(utils, "_sleep_with_cancel", lambda *_args, **_kwargs: None)
    debug_info = {}

    token = utils._fetch_cielo_token_from_gmail(
        timeout=0.1,
        min_internal_ts=500.0,
        debug_info=debug_info,
    )

    assert token is None
    assert debug_info["candidate_count"] == 0
    assert debug_info["fallback_candidate_count"] == 0


def test_build_card_reports_from_cielo_csv(tmp_path):
    caminho = tmp_path / "relatorio_cielo.csv"
    caminho.write_text(
        "\n".join(
            [
                "Relatorio Cielo",
                "Data da venda;Hora;Forma de pagamento;Status;Valor da venda;Codigo de autorizacao",
                "05/05/2026;13:45;Credito a vista;Aprovada;R$ 100,50;000123",
                "05/05/2026;14:10;Debito;Autorizada;50,00;000124",
                "05/05/2026;15:00;Credito;Cancelada;70,00;000125",
                "04/05/2026;16:00;Credito;Aprovada;20,00;000126",
            ]
        ),
        encoding="utf-8",
    )

    reports = _build_card_reports_from_cielo(str(caminho), "05/05/2026")

    assert reports["cartao_credito_caixa"]["quantidade_autorizados"] == 1
    assert reports["cartao_credito_caixa"]["total_autorizado"] == 100.50
    assert reports["cartao_debito_caixa"]["quantidade_autorizados"] == 1
    assert reports["cartao_debito_caixa"]["total_autorizado"] == 50.00


def test_mva_cielo_fallback_can_be_disabled(monkeypatch):
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("Cielo should not be consulted")

    monkeypatch.setattr(utils, "_find_local_cielo_card_report", fail_if_called)
    monkeypatch.setattr(utils, "baixar_relatorio_cielo_mva", fail_if_called)

    fechamento = {
        "relatorios_pagamento": {
            "cartao_credito": {
                "total_autorizado": 100.0,
                "itens_autorizados": [{"valor_bruto": 100.0}],
            },
            "cartao_credito_caixa": {
                "total_autorizado": 0.0,
                "itens_autorizados": [],
            },
        }
    }

    result, avisos = utils._integrate_cielo_card_reports_if_needed(
        fechamento,
        "09/06/2026",
        auto_download_missing=True,
        allow_cielo_fallback=False,
    )

    assert result is fechamento
    assert avisos == []


def test_empty_mva_budget_report_combines_without_changing_davs():
    davs = {
        "arquivo": "dav.pdf",
        "caixa_modelo": "MVA",
        "arquivo_tipo": "exportacao_dados_mva",
        "periodo": "25/05/2026 - 25/05/2026",
        "pedidos_total": 1,
        "pedidos_caixa": 1,
        "pedidos_excluidos": 0,
        "pedidos_editando": 0,
        "pedidos_outros_status": 0,
        "total_documento": 10.0,
        "total_excluido": 0.0,
        "total_caixa": 10.0,
        "itens_caixa": [{"pedido": "123456", "valor": 10.0, "ordem": "2026-05-25 08:00:00"}],
        "itens_excluidos": [],
    }

    combined = utils.combinar_relatorios_caixa_mva([davs, criar_relatorio_orcamentos_mva_vazio(davs["periodo"])])

    assert combined["pedidos_total"] == 1
    assert combined["total_caixa"] == 10.0
    assert combined["periodo"] == "25/05/2026 - 25/05/2026"


def test_build_card_reports_from_cielo_csv_prefers_transaction_header(tmp_path):
    caminho = tmp_path / "relatorio_cielo_detalhado.csv"
    caminho.write_text(
        "\n".join(
            [
                "Consolidado de vendas Cielo",
                "Formas de pagamento;Quantidade de vendas;Valor bruto;Taxa/tarifa;Valor liquido",
                "Credito a vista;43;2.013,25;-72,74;1.940,51",
                "Data da venda;Forma de pagamento;Quantidade de vendas;Valor bruto;Valor Taxa/Tarifa;Valor liquido",
                "30/04/2026;Credito a vista;43;2.013,25;-72,74;1.940,51",
                "Detalhamento de vendas Cielo",
                "Data da venda;Hora;Forma de pagamento;Status;Valor bruto;Codigo de autorizacao",
                "30/04/2026;10:20;Credito a vista;Aprovada;R$ 90,00;123456",
                "30/04/2026;11:15;Debito;Autorizada;R$ 40,00;123457",
            ]
        ),
        encoding="utf-8",
    )

    reports = _build_card_reports_from_cielo(str(caminho), "30/04/2026")

    assert reports["cartao_credito_caixa"]["quantidade_autorizados"] == 1
    assert reports["cartao_credito_caixa"]["total_autorizado"] == 90.00
    assert reports["cartao_debito_caixa"]["quantidade_autorizados"] == 1
    assert reports["cartao_debito_caixa"]["total_autorizado"] == 40.00


def test_wait_for_cielo_downloaded_report_accepts_requested_date_filename(tmp_path):
    caminho = tmp_path / "Vendas_Cielo_historico_resumo-20260430-20260430-1-1-csv.csv"
    caminho.write_text("arquivo resumido aguardando processamento\n", encoding="utf-8")

    found = _wait_for_cielo_downloaded_report(str(tmp_path), "30/04/2026", time.time() - 5, timeout=0.1)

    assert found == str(caminho)


def test_wait_for_cielo_downloaded_report_requires_detailed_when_requested(tmp_path):
    resumo = tmp_path / "Vendas_Cielo_historico_resumo-20260430-20260430-1-1-csv.csv"
    resumo.write_text(
        "\n".join(
            [
                "Consolidado de vendas Cielo",
                "Data da venda;Forma de pagamento;Quantidade de vendas;Valor bruto",
                "30/04/2026;Credito a vista;1;90,00",
            ]
        ),
        encoding="utf-8",
    )

    found = _wait_for_cielo_downloaded_report(str(tmp_path), "30/04/2026", time.time() - 5, timeout=0.1, require_detailed=True)

    assert found is None


def test_wait_for_cielo_downloaded_report_accepts_detailed_when_required(tmp_path):
    detalhado = tmp_path / "Vendas_cielo_historico_detalhe-20260430-20260430-1-1-csv.csv"
    detalhado.write_text(
        "\n".join(
            [
                "Detalhado de vendas Cielo",
                "Data da venda;Hora da venda;Forma de pagamento;Status da venda;Valor bruto;Código de autorização",
                "30/04/2026;10:20;Crédito à vista;Aprovada;90,00;123456",
            ]
        ),
        encoding="utf-8",
    )

    found = _wait_for_cielo_downloaded_report(str(tmp_path), "30/04/2026", time.time() - 5, timeout=0.1, require_detailed=True)

    assert found == str(detalhado)


def test_wait_for_cielo_downloaded_report_rejects_non_exact_cielo_range(tmp_path):
    detalhado = tmp_path / "Vendas_cielo_hoje_detalhe-20260618-20260619-1-1-csv.csv"
    detalhado.write_text(
        "\n".join(
            [
                "Detalhado de vendas Cielo",
                "Data da venda: 18/06/2026 a 19/06/2026",
                "Data da venda;Hora da venda;Forma de pagamento;Status da venda;Valor bruto;Codigo de autorizacao",
                "19/06/2026;14:20;Credito a vista;Aprovada;90,00;123456",
            ]
        ),
        encoding="utf-8",
    )

    found = _wait_for_cielo_downloaded_report(str(tmp_path), "19/06/2026", time.time() - 5, timeout=0.1, require_detailed=True)

    assert found is None


def test_find_local_cielo_card_report_rejects_non_exact_range(tmp_path, monkeypatch):
    detalhado = tmp_path / "cielo_cartoes_19062026_mva_auto.csv"
    detalhado.write_text(
        "\n".join(
            [
                "Detalhado de vendas Cielo",
                "Data da venda: 18/06/2026 a 19/06/2026",
                "Data da venda;Hora da venda;Forma de pagamento;Status da venda;Valor bruto;Codigo de autorizacao",
                "19/06/2026;14:20;Credito a vista;Aprovada;90,00;123456",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(utils, "_candidate_local_report_dirs", lambda: [tmp_path])

    found = utils._find_local_cielo_card_report("19/06/2026", company="MVA")

    assert found["cartoes"] is None
    assert any("intervalo" in aviso for aviso in found["avisos"])


def _write_caixa_pix_xlsx_with_invalid_styles(path: Path) -> None:
    strings = [
        "Relatorio de Vendas Pix",
        "Data da venda",
        "Cod. de autorizacao",
        "Valor bruto",
        "Terminal",
        "Numero do estabelecimento",
        "Status",
        "23/05/2026 as 10:01",
        "ABC123",
        "10,50",
        "POS1",
        "91111977",
        "APROVADA",
    ]
    shared_items = "".join(f"<si><t>{value}</t></si>" for value in strings)
    sheet_rows = "\n".join(
        [
            '<row r="1"><c r="A1" t="s"><v>0</v></c></row>',
            (
                '<row r="2">'
                '<c r="A2" t="s"><v>1</v></c>'
                '<c r="B2" t="s"><v>2</v></c>'
                '<c r="C2" t="s"><v>3</v></c>'
                '<c r="D2" t="s"><v>4</v></c>'
                '<c r="E2" t="s"><v>5</v></c>'
                '<c r="F2" t="s"><v>6</v></c>'
                "</row>"
            ),
            (
                '<row r="3">'
                '<c r="A3" t="s"><v>7</v></c>'
                '<c r="B3" t="s"><v>8</v></c>'
                '<c r="C3" t="s"><v>9</v></c>'
                '<c r="D3" t="s"><v>10</v></c>'
                '<c r="E3" t="s"><v>11</v></c>'
                '<c r="F3" t="s"><v>12</v></c>'
                "</row>"
            ),
        ]
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "[Content_Types].xml",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                '<Default Extension="xml" ContentType="application/xml"/>'
                '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
                '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
                '<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>'
                '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
                "</Types>"
            ),
        )
        archive.writestr(
            "_rels/.rels",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
                "</Relationships>"
            ),
        )
        archive.writestr(
            "xl/workbook.xml",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
                '<sheets><sheet name="Pix" sheetId="1" r:id="rId1"/></sheets>'
                "</workbook>"
            ),
        )
        archive.writestr(
            "xl/_rels/workbook.xml.rels",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
                '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>'
                '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
                "</Relationships>"
            ),
        )
        archive.writestr(
            "xl/sharedStrings.xml",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                f'<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="{len(strings)}" uniqueCount="{len(strings)}">'
                f"{shared_items}</sst>"
            ),
        )
        archive.writestr(
            "xl/worksheets/sheet1.xml",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                f"<sheetData>{sheet_rows}</sheetData></worksheet>"
            ),
        )
        archive.writestr("xl/styles.xml", "<styleSheet><broken></styleSheet>")


def test_wait_for_downloaded_report_accepts_pix_xlsx_with_invalid_styles(tmp_path):
    caminho = tmp_path / "Relatorio_de_Vendas_Pix_23-05-2026_1033.xlsx"
    _write_caixa_pix_xlsx_with_invalid_styles(caminho)

    found = utils._wait_for_downloaded_report(str(tmp_path), "23/05/2026", "pix", time.time() - 5, timeout=0.1)

    assert found is not None
    assert Path(found).suffix == ".csv"
    assert "23/05/2026 as 10:01" in Path(found).read_text(encoding="utf-8-sig")


def test_find_local_payment_reports_accepts_pix_xlsx_with_invalid_styles(tmp_path, monkeypatch):
    caminho = tmp_path / "Relatorio_de_Vendas_Pix_23-05-2026_eh_auto.xlsx"
    _write_caixa_pix_xlsx_with_invalid_styles(caminho)
    monkeypatch.setattr(utils, "_candidate_local_report_dirs", lambda: [tmp_path])

    found = _find_eh_local_payment_reports("23/05/2026", company="EH")

    assert found["pix"] == str(caminho)


def test_find_local_payment_reports_prefers_consolidated_company_card_xlsx(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    data_br = "10/05/2026"
    rows = [
        {
            "Data da venda": "10/05/2026 08:04:31",
            "Cód. de autorização": "123456",
            "Produto": "Débito",
            "Valor bruto": "31.00",
            "Status": "Aprovada",
        }
    ]
    consolidated = tmp_path / "Historico_Simplificado_de_vendas_10-05-2026_mva_auto.xlsx"
    establishment = tmp_path / "Historico_Simplificado_de_vendas_10-05-2026_mva_auto_est91119212.xlsx"
    pd.DataFrame(rows).to_excel(consolidated, index=False)
    pd.DataFrame(rows).to_excel(establishment, index=False)

    now = time.time()
    # The per-establishment file may be newer, but the consolidated file covers all terminals.
    import os

    os.utime(consolidated, (now - 10, now - 10))
    os.utime(establishment, (now, now))
    monkeypatch.chdir(tmp_path)

    found = _find_eh_local_payment_reports(data_br, company="MVA")

    assert found["cartoes"] == str(consolidated)


def test_find_local_payment_reports_ignores_cielo_files(tmp_path, monkeypatch):
    cielo = tmp_path / "cielo_cartoes_05062026_mva_auto.csv"
    cielo.write_text(
        "\n".join(
            [
                "Detalhado de vendas Cielo",
                "Data da venda;Hora da venda;Forma de pagamento;Status da venda;Valor bruto;Codigo de autorizacao",
                "05/06/2026;10:20;Credito a vista;Aprovada;100,00;123456",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(utils, "_candidate_local_report_dirs", lambda: [tmp_path])

    found = _find_eh_local_payment_reports("05/06/2026", company="MVA")

    assert found["pix"] is None
    assert found["cartoes"] is None
    assert found["avisos"] == []


def test_mva_cielo_gap_and_merge_card_machine_report():
    fechamento = {
        "relatorios_pagamento": {
            "cartao_credito": {"total_autorizado": 150.0},
            "cartao_credito_caixa": {"total_autorizado": 100.0},
            "cartao_debito": {"total_autorizado": 50.0},
            "cartao_debito_caixa": {"total_autorizado": 50.0},
        }
    }
    assert _mva_cielo_needed_card_keys(fechamento) == ["cartao_credito_caixa"]
    assert _mva_cielo_pending_card_count(fechamento) == 1

    merged = _merge_card_machine_report(
        {
            "categoria": "cartao_credito_caixa",
            "arquivo": "caixa.xlsx",
            "caminho": "caixa.xlsx",
            "itens_autorizados": [{"numero": "1", "data_venda": "05/05/2026 às 10:00", "valor_bruto": 100.0}],
        },
        {
            "categoria": "cartao_credito_caixa",
            "arquivo": "cielo.csv",
            "caminho": "cielo.csv",
            "itens_autorizados": [{"numero": "2", "data_venda": "05/05/2026 às 11:00", "valor_bruto": 50.0}],
        },
    )

    assert merged["total_autorizado"] == 150.0
    assert merged["quantidade_autorizados"] == 2
    assert merged["origem"] == "caixa_cielo_cartoes"


def test_mva_cielo_merge_only_closing_gap():
    relatorios = {
        "cartao_credito": {
            "total_autorizado": 150.0,
            "itens_autorizados": [
                {"valor_bruto": 100.0},
                {"valor_bruto": 50.0},
            ],
        },
        "cartao_credito_caixa": {
            "categoria": "cartao_credito_caixa",
            "arquivo": "caixa.xlsx",
            "caminho": "caixa.xlsx",
            "total_autorizado": 100.0,
            "itens_autorizados": [
                {"numero": "1", "data_venda": "05/05/2026 as 10:00", "valor_bruto": 100.0},
            ],
        },
    }
    cielo_reports = {
        "cartao_credito_caixa": {
            "categoria": "cartao_credito_caixa",
            "arquivo": "cielo.csv",
            "caminho": "cielo.csv",
            "itens_autorizados": [
                {"numero": "2", "data_venda": "05/05/2026 as 11:00", "valor_bruto": 50.0},
                {"numero": "3", "data_venda": "05/05/2026 as 12:00", "valor_bruto": 70.0},
            ],
        }
    }

    assert utils._merge_cielo_reports_into_payment_reports(relatorios, cielo_reports) is True

    merged = relatorios["cartao_credito_caixa"]
    assert merged["total_autorizado"] == 150.0
    assert merged["quantidade_autorizados"] == 2
    assert [item["valor_bruto"] for item in merged["itens_autorizados"]] == [100.0, 50.0]


def test_mva_cielo_pending_card_count_uses_unmatched_closing_items():
    fechamento = {
        "relatorios_pagamento": {
            "cartao_credito": {
                "total_autorizado": 180.0,
                "quantidade_autorizados": 3,
                "itens_autorizados": [
                    {"valor_bruto": 50.0},
                    {"valor_bruto": 60.0},
                    {"valor_bruto": 70.0},
                ],
            },
            "cartao_credito_caixa": {
                "total_autorizado": 50.0,
                "quantidade_autorizados": 1,
                "itens_autorizados": [{"valor_bruto": 50.0}],
            },
            "cartao_debito": {"total_autorizado": 0.0, "itens_autorizados": []},
            "cartao_debito_caixa": {"total_autorizado": 0.0, "itens_autorizados": []},
        }
    }

    assert _mva_cielo_pending_card_count(fechamento) == 2


def test_mva_cielo_same_day_threshold_triggers_download(tmp_path, monkeypatch):
    data_br = utils.datetime.now().strftime("%d/%m/%Y")
    calls = []
    fechamento = {
        "relatorios_pagamento": {
            "cartao_credito": {
                "total_autorizado": 1600.0,
                "itens_autorizados": [{"valor_bruto": float(valor)} for valor in range(1, 41)],
            },
            "cartao_credito_caixa": {
                "total_autorizado": 0.0,
                "itens_autorizados": [],
            },
        }
    }

    monkeypatch.setattr(utils, "_find_local_cielo_card_report", lambda data, company="MVA": {"cartoes": None, "avisos": []})
    monkeypatch.setattr(utils, "_new_cielo_debug_log_path", lambda data, company="MVA": tmp_path / "cielo.log")

    def fake_baixar(data, **kwargs):
        calls.append(data)
        return {"cartoes": None, "avisos": [], "debug_log": str(tmp_path / "download.log")}

    monkeypatch.setattr(utils, "baixar_relatorio_cielo_mva", fake_baixar)

    _result, avisos = utils._integrate_cielo_card_reports_if_needed(
        fechamento,
        data_br,
        auto_download_missing=True,
        company="MVA",
    )

    assert calls == [data_br]
    assert avisos == []


def test_mva_cielo_local_report_skips_download(tmp_path, monkeypatch):
    data_br = "09/05/2026"
    local_cielo = tmp_path / "cielo_cartoes_09052026_mva_auto.csv"
    local_cielo.write_text(
        "\n".join(
            [
                "Data da venda: 09/05/2026 a 09/05/2026",
                "Data da venda;Hora da venda;Produto;Status;Valor bruto;Codigo de autorizacao",
                "09/05/2026;10:15;Credito;Aprovada;100,00;ABC123",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(utils, "_candidate_local_report_dirs", lambda: [tmp_path])

    def fail_download(*args, **kwargs):
        raise AssertionError("Cielo download should not run when a local valid report exists")

    monkeypatch.setattr(utils, "baixar_relatorio_cielo_mva", fail_download)
    fechamento = {
        "relatorios_pagamento": {
            "cartao_credito": {
                "total_autorizado": 100.0,
                "itens_autorizados": [{"valor_bruto": 100.0}],
            },
            "cartao_credito_caixa": {
                "total_autorizado": 0.0,
                "itens_autorizados": [],
            },
        }
    }

    result, avisos = utils._integrate_cielo_card_reports_if_needed(
        fechamento,
        data_br,
        auto_download_missing=True,
        company="MVA",
    )

    assert avisos == []
    assert result["relatorios_pagamento"]["cartao_credito_caixa"]["total_autorizado"] == 100.0
    assert _mva_cielo_needed_card_keys(result) == []


def test_mva_refresh_needs_detects_low_caixa_reports_after_cielo():
    fechamento = {
        "relatorios_pagamento": {
            "pix_fechamento": {"total_autorizado": 250.0},
            "pix_caixa": {"total_autorizado": 20.0},
            "cartao_credito": {"total_autorizado": 300.0},
            "cartao_credito_caixa": {"total_autorizado": 100.0},
            "cartao_debito": {"total_autorizado": 80.0},
            "cartao_debito_caixa": {"total_autorizado": 80.0},
        }
    }

    needs = _mva_caixa_reports_refresh_needs(fechamento)

    assert needs["need_cartoes"] is True
    assert needs["need_pix"] is True
    assert any("cartao_credito" in reason for reason in needs["reasons"])
    assert any("pix" in reason for reason in needs["reasons"])


def test_mva_refresh_retries_zero_pix_without_cielo(monkeypatch):
    calls = []
    fechamento = {
        "relatorios_pagamento": {
            "pix_fechamento": {"total_autorizado": 250.0},
            "pix_caixa": {"total_autorizado": 0.0},
            "cartao_credito": {"total_autorizado": 20.0},
            "cartao_credito_caixa": {"total_autorizado": 20.0},
            "cartao_debito": {"total_autorizado": 30.0},
            "cartao_debito_caixa": {"total_autorizado": 30.0},
        }
    }

    def fake_baixar(data_br, **kwargs):
        calls.append(
            {
                "data_br": data_br,
                "need_pix": kwargs.get("need_pix"),
                "need_cartoes": kwargs.get("need_cartoes"),
            }
        )
        return {"avisos": []}

    def fake_integrate(report, data_br, **kwargs):
        report = dict(report)
        relatorios = dict(report.get("relatorios_pagamento") or {})
        relatorios["pix_caixa"] = {"total_autorizado": 250.0}
        report["relatorios_pagamento"] = relatorios
        return report, relatorios["pix_caixa"], []

    monkeypatch.setattr(utils, "baixar_relatorios_caixa_eh_azulzinha", fake_baixar)
    monkeypatch.setattr(utils, "_integrate_local_payment_reports", fake_integrate)

    result, avisos, refreshed = _refresh_mva_caixa_reports_if_needed(
        fechamento,
        "08/05/2026",
        auto_download_missing=True,
        company="MVA",
    )

    assert refreshed is True
    assert avisos == []
    assert calls == [{"data_br": "08/05/2026", "need_pix": True, "need_cartoes": False}]
    assert result["relatorios_pagamento"]["pix_caixa"]["total_autorizado"] == 250.0


def test_mva_clipp_scope_is_applied_before_payment_refresh(monkeypatch):
    texto = """
    MVA COMERCIO
    FECHAMENTO DE CAIXA
    DOCUMENTOS GERADOS
    PERIODO ANALISADO, DE 06/05/2026 ATE 06/05/2026
    1 - Abertura : 06/05/2026 08:00:00 - Fechamento : 06/05/2026 13:00:00
    2 - Abertura : 06/05/2026 13:00:01 - Fechamento : 06/05/2026 18:00:00
    CARTAO DE DEBITO: 1100,00
    PAGAMENTO INSTANTANEO (PIX): 0,00
    000001 NFCE 09:00:00 CLIENTE A CARTAO DE DEBITO 1000,00
    000002 NFCE 14:00:00 CLIENTE B CARTAO DE DEBITO 100,00
    """
    download_calls = []

    def fake_find_reports(data_br, **kwargs):
        return {"pix": "pix.csv", "cartoes": "card.xlsx", "avisos": []}

    def fake_integrate(report, data_br, **kwargs):
        assert report["relatorios_pagamento"]["cartao_debito"]["total_autorizado"] == 100.0
        report = dict(report)
        relatorios = dict(report.get("relatorios_pagamento") or {})
        relatorios["cartao_debito_caixa"] = {"total_autorizado": 100.0}
        report["relatorios_pagamento"] = relatorios
        return report, None, []

    def fail_download(*args, **kwargs):
        download_calls.append((args, kwargs))
        raise AssertionError("payment download should not run after scoped totals match")

    monkeypatch.setattr(utils, "_read_pdf_text", lambda _path: texto)
    monkeypatch.setattr(utils, "_find_eh_local_payment_reports", fake_find_reports)
    monkeypatch.setattr(utils, "_integrate_local_payment_reports", fake_integrate)
    monkeypatch.setattr(utils, "_find_local_cielo_card_report", lambda *args, **kwargs: {"cartoes": None, "avisos": []})
    monkeypatch.setattr(utils, "baixar_relatorios_caixa_eh_azulzinha", fail_download)
    monkeypatch.setattr(utils, "baixar_relatorio_cielo_mva", fail_download)

    report = analisar_pdf_fechamento_caixa_mva_clipp(
        "fechamento.pdf",
        auto_download_missing=True,
        scope_mode="afternoon",
    )

    assert report["relatorios_pagamento"]["cartao_debito"]["total_autorizado"] == 100.0
    assert report["relatorios_pagamento"]["cartao_debito_caixa"]["total_autorizado"] == 100.0
    assert _mva_caixa_reports_refresh_needs(report)["need_cartoes"] is False
    assert describe_closing_scope(report)["has_afternoon_only"] is True
    assert download_calls == []


def test_mva_afternoon_scope_ignores_short_operational_opening_window():
    relatorio_caixa = {
        "caixa_modelo": "MVA",
        "periodo": "05/06/2026 - 05/06/2026",
        "itens_caixa": [
            {"pedido": "000001", "valor": 10.0, "ordem": "2026-06-05 08:00:00"},
            {"pedido": "000002", "valor": 20.0, "ordem": "2026-06-05 14:00:00"},
        ],
        "itens_excluidos": [],
    }
    fechamento = {
        "caixa_modelo": "MVA",
        "periodo": "05/06/2026 - 05/06/2026",
        "fechamento_janelas": [
            {"abertura": "05/06/2026 07:52:16", "fechamento": "05/06/2026 07:52:44"},
            {"abertura": "05/06/2026 07:53:18", "fechamento": "05/06/2026 13:26:12"},
            {"abertura": "05/06/2026 13:26:53", "fechamento": "05/06/2026 17:31:14"},
        ],
        "nfces": [
            {
                "numero": "000001",
                "data_venda": "05/06/2026 08:00:00",
                "valor": 10.0,
            },
            {
                "numero": "000002",
                "data_venda": "05/06/2026 14:00:00",
                "valor": 20.0,
            },
        ],
        "relatorios_pagamento": {
            "dinheiro": {
                "categoria": "dinheiro",
                "total_autorizado": 30.0,
                "quantidade_autorizados": 2,
                "itens_autorizados": [
                    {"numero": "000001", "data_venda": "05/06/2026 08:00:00", "valor_bruto": 10.0},
                    {"numero": "000002", "data_venda": "05/06/2026 14:00:00", "valor_bruto": 20.0},
                ],
            }
        },
    }

    relatorio_tarde, fechamento_tarde, _ = aplicar_escopo_relatorio_caixa(
        relatorio_caixa,
        fechamento,
        None,
        scope_mode="afternoon",
    )

    assert [item["pedido"] for item in relatorio_tarde["itens_caixa"]] == ["000002"]
    assert [item["numero"] for item in fechamento_tarde["nfces"]] == ["000002"]
    assert fechamento_tarde["relatorios_pagamento"]["dinheiro"]["total_autorizado"] == 20.0
    assert fechamento_tarde["fechamento_janelas"] == [
        {"abertura": "05/06/2026 13:26:53", "fechamento": "05/06/2026 17:31:14"}
    ]

    relatorio_manha, fechamento_manha, _ = aplicar_escopo_relatorio_caixa(
        relatorio_caixa,
        fechamento,
        None,
        scope_mode="morning",
    )

    assert [item["pedido"] for item in relatorio_manha["itens_caixa"]] == ["000001"]
    assert [item["numero"] for item in fechamento_manha["nfces"]] == ["000001"]
    assert fechamento_manha["relatorios_pagamento"]["dinheiro"]["total_autorizado"] == 10.0
    assert fechamento_manha["fechamento_janelas"] == [
        {"abertura": "05/06/2026 07:53:18", "fechamento": "05/06/2026 13:26:12"}
    ]


def test_morning_scope_does_not_reuse_afternoon_only_window():
    windows = [{"abertura": "05/06/2026 13:26:53", "fechamento": "05/06/2026 17:31:14"}]

    assert utils._scope_windows_for_mode(windows, "morning") == []
    assert utils._scope_windows_for_mode(windows, "afternoon") == windows


def test_mva_next_day_closing_filter_keeps_target_opening_date(monkeypatch):
    texto = """
    MVA COMERCIO
    FECHAMENTO DE CAIXA
    DOCUMENTOS GERADOS
    PERIODO ANALISADO, DE 14/05/2026 ATE 14/05/2026
    1 - Abertura : 13/05/2026 13:27:54 - Fechamento : 14/05/2026 08:00:30
    2 - Abertura : 14/05/2026 08:02:09 - Fechamento : 14/05/2026 13:26:29
    CARTAO DE DEBITO: 300,00
    PAGAMENTO INSTANTANEO (PIX): 0,00
    000001 NFCE 07:50:00 CLIENTE A CARTAO DE DEBITO 100,00
    000002 NFCE 09:00:00 CLIENTE B CARTAO DE DEBITO 200,00
    """
    monkeypatch.setattr(utils, "_read_pdf_text", lambda _path: texto)

    report = analisar_pdf_fechamento_caixa_mva_clipp(
        "fechamento.pdf",
        auto_download_missing=False,
        filter_opening_date_br="14/05/2026",
    )

    assert report["periodo"] == "14/05/2026 - 14/05/2026"
    assert report["quantidade_nfce"] == 1
    assert report["total_nfce"] == 200.0
    assert report["fechamento_janelas"] == [
        {"abertura": "14/05/2026 08:02:09", "fechamento": "14/05/2026 13:26:29"}
    ]
    assert report["relatorios_pagamento"]["cartao_debito"]["total_autorizado"] == 200.0


def test_eh_next_day_closing_filter_keeps_target_sales_and_afternoon_scope():
    fechamento = {
        "periodo": "13/05/2026 - 14/05/2026",
        "fechamento_janelas": [
            {"abertura": "13/05/2026 08:00:00", "fechamento": "13/05/2026 12:00:00"},
            {"abertura": "13/05/2026 18:00:00", "fechamento": "14/05/2026 08:00:00"},
            {"abertura": "14/05/2026 08:00:00", "fechamento": "14/05/2026 12:00:00"},
        ],
        "nfces": [
            {
                "numero": "000001",
                "data_venda": "13/05/2026",
                "valor": 10.0,
                "scope_abertura": "13/05/2026 08:00:00",
                "scope_fechamento": "13/05/2026 12:00:00",
            },
            {
                "numero": "000002",
                "data_venda": "13/05/2026",
                "valor": 20.0,
                "scope_abertura": "13/05/2026 18:00:00",
                "scope_fechamento": "14/05/2026 08:00:00",
            },
            {
                "numero": "000003",
                "data_venda": "14/05/2026",
                "valor": 30.0,
                "scope_abertura": "14/05/2026 08:00:00",
                "scope_fechamento": "14/05/2026 12:00:00",
            },
        ],
        "relatorios_pagamento": {
            "pix_fechamento": {
                "forma_pagamento": "PIX",
                "summary_label": "PIX",
                "itens_autorizados": [
                    {
                        "numero": "000001",
                        "data_venda": "13/05/2026",
                        "valor_bruto": 10.0,
                        "scope_abertura": "13/05/2026 08:00:00",
                        "scope_fechamento": "13/05/2026 12:00:00",
                    },
                    {
                        "numero": "000002",
                        "data_venda": "13/05/2026",
                        "valor_bruto": 20.0,
                        "scope_abertura": "13/05/2026 18:00:00",
                        "scope_fechamento": "14/05/2026 08:00:00",
                    },
                    {
                        "numero": "000003",
                        "data_venda": "14/05/2026",
                        "valor_bruto": 30.0,
                        "scope_abertura": "14/05/2026 08:00:00",
                        "scope_fechamento": "14/05/2026 12:00:00",
                    },
                ],
            }
        },
    }
    relatorio_caixa = {
        "caixa_modelo": "EH",
        "periodo": "13/05/2026 - 13/05/2026",
        "itens_caixa": [
            {"pedido": "000001", "valor": 10.0, "cliente": "BALCAO"},
            {"pedido": "000002", "valor": 20.0, "cliente": "BALCAO"},
            {"pedido": "000003", "valor": 30.0, "cliente": "BALCAO"},
        ],
        "itens_excluidos": [],
    }

    filtrado = _filter_zweb_fechamento_to_sales_date(fechamento, "13/05/2026")

    assert filtrado["periodo"] == "13/05/2026 - 13/05/2026"
    assert [item["numero"] for item in filtrado["nfces"]] == ["000001", "000002"]
    assert len(filtrado["fechamento_janelas"]) == 2
    assert describe_closing_scope(filtrado)["has_full_day"] is True
    assert filtrado["relatorios_pagamento"]["pix_fechamento"]["total_autorizado"] == 30.0

    relatorio_tarde, fechamento_tarde, _ = aplicar_escopo_relatorio_caixa(
        relatorio_caixa,
        filtrado,
        None,
        scope_mode="afternoon",
    )

    assert [item["pedido"] for item in relatorio_tarde["itens_caixa"]] == ["000002"]
    assert [item["numero"] for item in fechamento_tarde["nfces"]] == ["000002"]

    relatorio_manha, fechamento_manha, _ = aplicar_escopo_relatorio_caixa(
        relatorio_caixa,
        filtrado,
        None,
        scope_mode="morning",
    )

    assert [item["pedido"] for item in relatorio_manha["itens_caixa"]] == ["000001"]
    assert [item["numero"] for item in fechamento_manha["nfces"]] == ["000001"]

    somente_tarde = _filter_zweb_fechamento_to_sales_date(
        {**fechamento, "nfces": [fechamento["nfces"][1]], "relatorios_pagamento": {}},
        "13/05/2026",
    )
    assert describe_closing_scope(somente_tarde)["has_afternoon_only"] is True


def test_eh_same_day_closing_filter_drops_previous_day_overnight_values():
    fechamento = {
        "periodo": "14/05/2026 - 14/05/2026",
        "fechamento_janelas": [
            {"abertura": "13/05/2026 13:27:54", "fechamento": "14/05/2026 08:00:30"},
            {"abertura": "14/05/2026 08:02:09", "fechamento": "14/05/2026 13:26:29"},
        ],
        "nfces": [
            {
                "numero": "000001",
                "data_venda": "13/05/2026",
                "valor": 100.0,
                "scope_abertura": "13/05/2026 13:27:54",
                "scope_fechamento": "14/05/2026 08:00:30",
            },
            {
                "numero": "000002",
                "data_venda": "14/05/2026",
                "valor": 200.0,
                "scope_abertura": "14/05/2026 08:02:09",
                "scope_fechamento": "14/05/2026 13:26:29",
            },
        ],
        "relatorios_pagamento": {
            "pix_fechamento": {
                "forma_pagamento": "PIX",
                "summary_label": "PIX",
                "itens_autorizados": [
                    {
                        "numero": "000001",
                        "data_venda": "13/05/2026",
                        "valor_bruto": 100.0,
                        "scope_abertura": "13/05/2026 13:27:54",
                        "scope_fechamento": "14/05/2026 08:00:30",
                    },
                    {
                        "numero": "000002",
                        "data_venda": "14/05/2026",
                        "valor_bruto": 200.0,
                        "scope_abertura": "14/05/2026 08:02:09",
                        "scope_fechamento": "14/05/2026 13:26:29",
                    },
                ],
            }
        },
    }

    assert _zweb_fechamento_has_sales_outside_date(fechamento, "14/05/2026") is True

    filtrado = _filter_zweb_fechamento_to_sales_date(fechamento, "14/05/2026")

    assert [item["numero"] for item in filtrado["nfces"]] == ["000002"]
    assert filtrado["total_nfce"] == 200.0
    assert filtrado["relatorios_pagamento"]["pix_fechamento"]["total_autorizado"] == 200.0
    assert filtrado["fechamento_janelas"] == [
        {"abertura": "14/05/2026 08:02:09", "fechamento": "14/05/2026 13:26:29"}
    ]
    assert describe_closing_scope(filtrado)["has_morning_only"] is True


def test_eh_cancelled_fiscal_coupons_absent_from_orders_are_visible():
    relatorio = {
        "caixa_modelo": "EH",
        "periodo": "14/05/2026 - 14/05/2026",
        "pedidos_caixa": 2,
        "pedidos_excluidos": 0,
        "pedidos_excluidos_cancelados": 0,
        "total_documento": 150.0,
        "total_excluido": 0.0,
        "total_excluido_cancelados": 0.0,
        "total_caixa": 150.0,
        "itens_caixa": [
            {"pedido": "000103230", "cliente": "CLIENTE BALCÃO", "documento": "NFC-e", "valor": 50.0},
            {"pedido": "000103244", "cliente": "CLIENTE BALCÃO", "documento": "NFC-e", "valor": 100.0},
        ],
        "itens_excluidos": [],
    }
    fiscal_status_map = {
        "000103230": {"cancelada": True, "valor": 50.0},
        "000103231": {"cancelada": True, "valor": 12.5},
        "000103244": {"cancelada": False, "valor": 100.0},
    }

    filtrado = utils._aplicar_filtro_canceladas_pedidos_eh(relatorio, fiscal_status_map)

    assert filtrado["pedidos_caixa"] == 1
    assert filtrado["pedidos_excluidos"] == 2
    assert filtrado["pedidos_excluidos_cancelados"] == 2
    assert filtrado["total_caixa"] == 100.0
    assert filtrado["total_excluido"] == 62.5
    assert filtrado["total_excluido_cancelados"] == 62.5
    assert filtrado["total_documento"] == 162.5
    assert [item["pedido"] for item in filtrado["itens_caixa"]] == ["000103244"]
    assert {item["pedido"] for item in filtrado["itens_excluidos"]} == {"000103230", "000103231"}


def test_eh_scope_filter_recalculates_cancelled_coupon_totals():
    window = {"abertura": "14/05/2026 08:02:09", "fechamento": "14/05/2026 13:26:29"}
    relatorio_caixa = {
        "caixa_modelo": "EH",
        "periodo": "14/05/2026 - 14/05/2026",
        "itens_caixa": [
            {"pedido": "000103231", "cliente": "CLIENTE BALCÃO", "valor": 100.0},
        ],
        "itens_excluidos": [
            {
                "pedido": "000103230",
                "cliente": "CLIENTE BALCÃO",
                "documento": "NFC-e cancelada",
                "motivo": "Cupom cancelado",
                "valor": 50.0,
            },
            {
                "pedido": "000103260",
                "cliente": "CLIENTE BALCÃO",
                "documento": "NFC-e cancelada",
                "motivo": "Cupom cancelado",
                "valor": 10.0,
            },
        ],
    }
    relatorio_fechamento = {
        "nfces": [
            {"numero": "000103229", "scope_abertura": window["abertura"], "scope_fechamento": window["fechamento"]},
            {"numero": "000103231", "scope_abertura": window["abertura"], "scope_fechamento": window["fechamento"]},
        ],
    }

    filtrado = utils._filter_eh_caixa_report_to_scope(relatorio_caixa, relatorio_fechamento, [window])

    assert filtrado["pedidos_excluidos_cancelados"] == 1
    assert filtrado["total_excluido_cancelados"] == 50.0
    assert [item["pedido"] for item in filtrado["itens_excluidos"]] == ["000103230"]


def test_eh_conciliation_report_exists_when_everything_matches():
    fechamento = utils.comparar_caixa_resumo_nfce(
        {
            "caixa_modelo": "EH",
            "periodo": "19/06/2026 - 19/06/2026",
            "total_caixa": 300.0,
            "itens_caixa": [
                {"pedido": "000103230", "cliente": "CLIENTE BALCAO", "valor": 100.0},
                {"pedido": "000103231", "cliente": "CLIENTE BALCAO", "valor": 200.0},
            ],
            "itens_excluidos": [],
        },
        {
            "resumo_modelo": "EH",
            "periodo": "19/06/2026 - 19/06/2026",
            "total_nfce": 300.0,
            "nfces": [
                {"numero": "000103230", "numero_exibicao": "103230", "valor": 100.0},
                {"numero": "000103231", "numero_exibicao": "103231", "valor": 200.0},
            ],
            "relatorios_pagamento": {
                "pix_fechamento": {
                    "categoria": "pix_fechamento",
                    "total_autorizado": 100.0,
                    "itens_autorizados": [
                        {"numero": "000103230", "numero_exibicao": "103230", "valor_bruto": 100.0},
                    ],
                },
                "pix_caixa": {
                    "categoria": "pix_caixa",
                    "total_autorizado": 100.0,
                    "itens_autorizados": [
                        {"data_venda": "19/06/2026 14:00:00", "valor_bruto": 100.0},
                    ],
                },
                "cartao_credito": {
                    "categoria": "cartao_credito",
                    "total_autorizado": 200.0,
                    "itens_autorizados": [
                        {"numero": "000103231", "numero_exibicao": "103231", "valor_bruto": 200.0},
                    ],
                },
                "cartao_credito_caixa": {
                    "categoria": "cartao_credito_caixa",
                    "total_autorizado": 200.0,
                    "itens_autorizados": [
                        {"data_venda": "19/06/2026 14:05:00", "valor_bruto": 200.0},
                    ],
                },
            },
        },
    )

    alertas = fechamento["relatorios_pagamento"]["alertas_eh"]

    assert fechamento["status"] == "Confere"
    assert fechamento["alertas_count"] == 0
    assert alertas["hidden_in_menu"] is True
    assert alertas["correlacao_rows"] == [
        ("Dinheiro", "R$ 0,00", "-", "Interno"),
        ("PIX", "R$ 100,00", "R$ 100,00", "Finalizado"),
        ("Cartão Crédito", "R$ 200,00", "R$ 200,00", "Finalizado"),
    ]


def test_mva_clipp_cancelled_cash_coupons_are_visible(monkeypatch):
    status_map = {
        "000388060": {
            "numero": "000388060",
            "numero_exibicao": "388060",
            "valor": 5.70,
            "cancelada": True,
        },
        "000388072": {
            "numero": "000388072",
            "numero_exibicao": "388072",
            "valor": 63.90,
            "cancelada": True,
        },
        "000388166": {
            "numero": "000388166",
            "numero_exibicao": "388166",
            "valor": 1.30,
            "cancelada": True,
        },
    }
    monkeypatch.setattr(
        utils,
        "_load_minhas_notas_mva_context",
        lambda periodo: ([], status_map, None),
    )

    fechamento = utils.comparar_caixa_resumo_nfce(
        {
            "periodo": "07/05/2026 - 07/05/2026",
            "caixa_modelo": "MVA",
            "itens_caixa": [],
            "total_caixa": 0.0,
        },
        {
            "arquivo_tipo": "fechamento_caixa_clipp_mva",
            "periodo": "07/05/2026 - 07/05/2026",
            "total_nfce": 70.90,
            "nfces": [{"numero": numero} for numero in status_map],
            "relatorios_pagamento": {
                "dinheiro": {
                    "categoria": "dinheiro",
                    "total_autorizado": 70.90,
                    "itens_autorizados": [
                        {
                            "numero": numero,
                            "numero_exibicao": dados["numero_exibicao"],
                            "valor_bruto": dados["valor"],
                        }
                        for numero, dados in status_map.items()
                    ],
                }
            },
        },
    )

    alertas = fechamento["relatorios_pagamento"]["alertas_eh"]

    assert fechamento["cupons_cancelados_count"] == 3
    assert fechamento["cupons_cancelados_valor"] == 70.90
    assert fechamento["valor_faltantes"] == 0.0
    assert fechamento["alertas_count"] == 0
    assert fechamento["status"] == "Confere"
    assert alertas["quantidade_relatorio"] == 0
    assert alertas["total_relatorio"] == 0.0
    assert alertas["cancelados_rows"] == [
        ("CF 388060", "R$ 5,70"),
        ("CF 388072", "R$ 63,90"),
        ("CF 388166", "R$ 1,30"),
    ]
