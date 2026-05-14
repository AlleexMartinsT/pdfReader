import time

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
    _is_eh_counter_client,
    _merge_card_machine_report,
    _mva_caixa_reports_refresh_needs,
    _mva_cielo_pending_card_count,
    _mva_cielo_needed_card_keys,
    _normalize_fiscal_number,
    _refresh_mva_caixa_reports_if_needed,
    _wait_for_cielo_downloaded_report,
    _zweb_fechamento_has_sales_outside_date,
    analisar_pdf_fechamento_caixa_mva_clipp,
    aplicar_escopo_relatorio_caixa,
    describe_closing_scope,
)

def test_parse_number():
    assert parse_number("R$ 1.500,20") == 1500.20
    assert parse_number("1.500,20") == 1500.20
    assert parse_number("1,500.20") == 1500.20
    assert parse_number("20,50") == 20.50
    assert parse_number("100.00") == 100.00
    assert parse_number(15.5) == 15.5
    assert parse_number(None) == 0.0
    assert parse_number(" ") == 0.0

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
