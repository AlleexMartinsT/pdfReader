from pydantic import BaseModel, Field
from typing import List, Optional

class ItemCaixa(BaseModel):
    pedido: str = Field(description="NÃºmero do pedido importado ou documento")
    cliente: str = Field(description="Nome limpo do cliente")
    documento: str = Field(description="Tipo de documento classificado. Ex: Nota Fiscal EletrÃ´nica")
    valor: float = Field(description="Valor monetÃ¡rio aferido")

class ItemExcluido(BaseModel):
    pedido: str
    cliente: str
    documento: str
    motivo: str = Field(description="Motivo fÃ­sico pelo qual o item foi deduzido do caixa")
    valor: float

class RelatorioCaixa(BaseModel):
    arquivo: str
    caixa_modelo: str = Field(pattern="^(EH|MVA)$", description="Modelo de layout original (MVA ou EH)")
    arquivo_tipo: str = Field(description="Ex: pedidos_importados_eh, fechamento_caixa_eh, etc.")
    periodo: Optional[str] = None
    
    pedidos_total: int = 0
    pedidos_balcao: int = 0
    pedidos_caixa: int = 0
    pedidos_excluidos: int = 0
    pedidos_excluidos_cliente: int = 0
    pedidos_excluidos_documento: int = 0
    pedidos_excluidos_cancelados: int = 0
    
    total_documento: float = 0.0
    total_excluido: float = 0.0
    total_excluido_cancelados: float = 0.0
    total_caixa: float = 0.0
    
    itens_caixa: List[ItemCaixa] = Field(default_factory=list)
    itens_excluidos: List[ItemExcluido] = Field(default_factory=list)

class RelatorioResumoNFCE(BaseModel):
    arquivo: str
    caixa_modelo: str = Field(pattern="^(EH|MVA)$")
    arquivo_tipo: str = Field(description="Deve ser resumo_nfce_eh ou resumo_nfce_mva")
    periodo: Optional[str] = None
    total_bruto_contabil: float = 0.0
    total_cancelamentos: float = 0.0
    total_descontos: float = 0.0
    total_liquido_contabil: float = 0.0
