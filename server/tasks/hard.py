"""Task 3 (Hard): Ambiguous 'top customers' question requiring data exploration."""

HARD_TASK = {
    "id": "hard",
    "name": "Customer Spending Analysis",
    "description": "Hard - Ambiguous question + CTE + window functions + NULL handling",
    "question": (
        "Management wants to identify the top customers in each country. For each customer "
        "who has placed at least 2 completed orders, show:\n"
        "1. Their name and country\n"
        "2. Their total lifetime spend (as total_spend, rounded to 2 decimals)\n"
        "3. Their average order value (as avg_order_value, rounded to 2 decimals) — "
        "total spend divided by number of distinct orders\n"
        "4. Their rank within their country by total spend, highest first (as country_rank)\n"
        "5. What percentage of their country's qualifying spend they represent "
        "(as pct_of_country, rounded to 1 decimal)\n\n"
        "Important: Only count revenue from orders that were actually fulfilled. "
        "Some customers have NULL emails — include them anyway (rank by spend, not profile completeness). "
        "Order by country, then rank."
    ),
    "expected_sql": (
        "WITH customer_totals AS ( "
        "SELECT c.id, c.name, c.country, "
        "ROUND(SUM(oi.quantity * oi.unit_price), 2) AS total_spend, "
        "COUNT(DISTINCT o.id) AS order_count "
        "FROM customers c "
        "JOIN orders o ON c.id = o.customer_id "
        "JOIN order_items oi ON o.id = oi.order_id "
        "WHERE o.status = 'completed' "
        "GROUP BY c.id, c.name, c.country "
        "HAVING COUNT(DISTINCT o.id) >= 2 "
        ") "
        "SELECT name, country, total_spend, "
        "ROUND(total_spend / order_count, 2) AS avg_order_value, "
        "RANK() OVER (PARTITION BY country ORDER BY total_spend DESC) AS country_rank, "
        "ROUND(total_spend * 100.0 / SUM(total_spend) OVER (PARTITION BY country), 1) AS pct_of_country "
        "FROM customer_totals "
        "ORDER BY country ASC, country_rank ASC"
    ),
    "expected_columns": ["name", "country", "total_spend", "avg_order_value", "country_rank", "pct_of_country"],
    "order_matters": True,
    "max_steps": 15,
    "max_submissions": 3,
    "relevant_tables": ["customers", "orders", "order_items"],
}
