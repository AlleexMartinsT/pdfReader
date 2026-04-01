import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils import _analisar_html_pedidos_importados_eh


def test_simular_novo_parser_html():
    mock_html = """
    <html>
        <body>
            <div class="mt-3" style="border: 1.5px solid #A4A5A7;">
                <div>
                    <span>Número do pedido:</span><span class="fw-bold">123456</span>
                    <span>Cliente:</span><span>CLIENTE BALCAO</span>
                </div>
                <table>
                    <tbody>
                        <tr>
                            <td><span>N102030</span></td>
                            <td>CUPOM FISCAL</td>
                            <td>R$ 1.550,25</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </body>
    </html>
    """

    resultado = _analisar_html_pedidos_importados_eh(mock_html)

    assert resultado["total_documento"] == 1550.25
    assert resultado["pedidos_caixa"] == 1
    assert resultado["itens_caixa"][0]["cliente"] == "CLIENTE BALCAO"
    assert resultado["itens_caixa"][0]["pedido"] == "000102030"
