
from advanced_order_generator import (
    generate_order,
    validate_order
)

# Test 1: Valid order
order = generate_order(2001)

assert validate_order(order) is True
print("Test 1 passed: Valid order accepted")

# Test 2: Invalid quantity
bad_order = order.copy()
bad_order["quantity"] = -5

assert validate_order(bad_order) is False
print("Test 2 passed: Negative quantity rejected")

# Test 3: Incorrect total
bad_total = order.copy()
bad_total["total_amount"] = -100

assert validate_order(bad_total) is False
print("Test 3 passed: Incorrect total rejected")

# Test 4: Missing required field
missing_field = order.copy()
del missing_field["customer_id"]

assert validate_order(missing_field) is False
print("Test 4 passed: Missing field rejected")

print("All tests passed!")
