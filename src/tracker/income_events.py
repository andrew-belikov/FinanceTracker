from typing import Optional


def compute_income_net_amount(gross_sum: float, tax_sum: float) -> float:
    return gross_sum + tax_sum


def compute_income_net_yield_pct(net_amount: float, cost_basis: Optional[float]) -> float:
    if cost_basis is None or cost_basis <= 0:
        return 0.0
    return net_amount / cost_basis * 100.0
