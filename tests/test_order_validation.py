
from advanced_order_generator import generate_order, validate_order


def test_valid_order():
    order = generate_order(1001)

    assert validate_order(order) is True


def test_invalid_quantity():
    order = generate_order(1002)

    order["quantity"] = 0

    assert validate_order(order) is False


def test_missing_required_field():
    order = generate_order(1003)

    del order["customer_id"]

    assert validate_order(order) is False


def test_incorrect_total_amount():
    order = generate_order(1004)

    order["total_amount"] = -100.00

    assert validate_order(order) is False
