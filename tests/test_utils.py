import pytest
from utils import parse_number, format_number_br, _classify_caixa_document, _is_eh_counter_client, _normalize_fiscal_number

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
