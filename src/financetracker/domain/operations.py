"""Domain classification of broker operation types."""


DEPOSIT_TYPES = frozenset({"OPERATION_TYPE_INPUT"})
WITHDRAWAL_TYPES = frozenset({"OPERATION_TYPE_OUTPUT"})
BUY_TYPES = frozenset({"OPERATION_TYPE_BUY", "OPERATION_TYPE_BUY_CARD"})
SELL_TYPES = frozenset({"OPERATION_TYPE_SELL"})
COMMISSION_TYPES = frozenset({"OPERATION_TYPE_BROKER_FEE", "OPERATION_TYPE_MARGIN_FEE", "OPERATION_TYPE_SUCCESS_FEE", "OPERATION_TYPE_WITHDRAW_COMMISSION", "OPERATION_TYPE_OTHER_FEE"})
INCOME_TAX_TYPES = frozenset({"OPERATION_TYPE_TAX", "OPERATION_TYPE_TAX_PROGRESSIVE", "OPERATION_TYPE_TAX_COUPON", "OPERATION_TYPE_TAX_DIVIDEND", "OPERATION_TYPE_COUPON_TAX", "OPERATION_TYPE_BOND_TAX", "OPERATION_TYPE_BOND_TAX_PROGRESSIVE", "OPERATION_TYPE_DIVIDEND_TAX", "OPERATION_TYPE_DIVIDEND_TAX_PROGRESSIVE"})
INCOME_EVENT_TAX_TYPES = frozenset({"OPERATION_TYPE_COUPON_TAX", "OPERATION_TYPE_BOND_TAX", "OPERATION_TYPE_BOND_TAX_PROGRESSIVE", "OPERATION_TYPE_DIVIDEND_TAX", "OPERATION_TYPE_DIVIDEND_TAX_PROGRESSIVE"})


def classify_operation_group(operation_type: str | None) -> str:
    operation_type = (operation_type or "").strip()
    if operation_type in DEPOSIT_TYPES: return "deposit"
    if operation_type in WITHDRAWAL_TYPES: return "withdrawal"
    if operation_type in BUY_TYPES: return "buy"
    if operation_type in SELL_TYPES: return "sell"
    if operation_type in COMMISSION_TYPES: return "commission"
    if operation_type in INCOME_TAX_TYPES: return "income_tax"
    if operation_type == "OPERATION_TYPE_DIVIDEND": return "dividend"
    if operation_type == "OPERATION_TYPE_COUPON": return "coupon"
    return "other"


def is_income_event_backed_tax_operation(operation_type: str | None) -> bool:
    return (operation_type or "").strip() in INCOME_EVENT_TAX_TYPES
