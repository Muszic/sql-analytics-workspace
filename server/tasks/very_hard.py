"""Task 4 (Very Hard): Month-over-month revenue trend analysis with nested CTEs, LAG, CASE WHEN."""

VERY_HARD_TASK = {
    "id": "very_hard",
    "name": "Revenue Trend Analysis",
    "description": "Very Hard - Nested CTEs + LAG() + COALESCE + CASE WHEN + strftime",
    "question": (
        "Analyze month-over-month revenue trends. For each month that has at least one "
        "completed order, calculate:\n"
        "1. The month (as 'YYYY-MM' format, column name: month)\n"
        "2. That month's total revenue (quantity * unit_price, rounded to 2 decimals, column: revenue)\n"
        "3. The previous month's revenue (column: prev_revenue — use 0 if there is no previous month)\n"
        "4. The percentage change from the previous month (column: growth_pct, rounded to 1 decimal — "
        "use 0 if there is no previous month)\n"
        "5. A trend classification (column: trend) based on these rules:\n"
        "   - 'first_month' if there is no previous month\n"
        "   - 'high_growth' if growth_pct > 20\n"
        "   - 'moderate_growth' if growth_pct > 0 and growth_pct <= 20\n"
        "   - 'flat' if growth_pct = 0\n"
        "   - 'decline' if growth_pct < 0\n\n"
        "Use strftime('%Y-%m', order_date) for month extraction. Only include completed orders. "
        "Use LAG() to get the previous month's revenue. Use COALESCE to handle NULLs. "
        "Order by month ascending."
    ),
    "expected_sql": (
        "WITH monthly_revenue AS ( "
        "SELECT strftime('%Y-%m', o.order_date) AS month, "
        "ROUND(SUM(oi.quantity * oi.unit_price), 2) AS revenue "
        "FROM orders o "
        "JOIN order_items oi ON o.id = oi.order_id "
        "WHERE o.status = 'completed' "
        "GROUP BY strftime('%Y-%m', o.order_date) "
        "), "
        "with_growth AS ( "
        "SELECT month, revenue, "
        "LAG(revenue) OVER (ORDER BY month) AS prev_revenue, "
        "ROUND((revenue - LAG(revenue) OVER (ORDER BY month)) * 100.0 "
        "/ LAG(revenue) OVER (ORDER BY month), 1) AS growth_pct "
        "FROM monthly_revenue "
        ") "
        "SELECT month, revenue, "
        "COALESCE(prev_revenue, 0) AS prev_revenue, "
        "COALESCE(growth_pct, 0) AS growth_pct, "
        "CASE "
        "WHEN prev_revenue IS NULL THEN 'first_month' "
        "WHEN growth_pct > 20 THEN 'high_growth' "
        "WHEN growth_pct > 0 THEN 'moderate_growth' "
        "WHEN growth_pct = 0 THEN 'flat' "
        "ELSE 'decline' "
        "END AS trend "
        "FROM with_growth "
        "ORDER BY month"
    ),
    "expected_columns": ["month", "revenue", "prev_revenue", "growth_pct", "trend"],
    "order_matters": True,
    "max_steps": 20,
    "max_submissions": 3,
    "relevant_tables": ["orders", "order_items"],
}
