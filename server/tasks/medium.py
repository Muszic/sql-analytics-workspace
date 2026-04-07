"""Task 2 (Medium): Ambiguous revenue question with dirty data traps."""

MEDIUM_TASK = {
    "id": "medium",
    "name": "Revenue Analysis",
    "description": "Medium - Ambiguous question + dirty data traps (cancelled orders, price vs unit_price)",
    "question": (
        "What was the total revenue per product category in Q4 2023 (October through December)? "
        "Include only categories that generated more than $1000. "
        "Show the category, the number of unique buyers, and the total revenue. "
        "Sort by revenue, highest first.\n\n"
        "Hint: Be careful about which orders should count toward revenue and which price "
        "column reflects what was actually charged."
    ),
    "expected_sql": (
        "SELECT p.category, "
        "COUNT(DISTINCT o.customer_id) AS customer_count, "
        "ROUND(SUM(oi.quantity * oi.unit_price), 2) AS total_revenue "
        "FROM orders o "
        "JOIN order_items oi ON o.id = oi.order_id "
        "JOIN products p ON oi.product_id = p.id "
        "WHERE o.order_date >= '2023-10-01' AND o.order_date <= '2023-12-31' "
        "AND o.status = 'completed' "
        "GROUP BY p.category "
        "HAVING SUM(oi.quantity * oi.unit_price) > 1000 "
        "ORDER BY total_revenue DESC"
    ),
    "expected_columns": ["category", "customer_count", "total_revenue"],
    "order_matters": True,
    "max_steps": 15,
    "max_submissions": 3,
    "relevant_tables": ["orders", "order_items", "products"],
}
