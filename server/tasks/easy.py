"""Task 1 (Easy): Single-table query with NULL data trap."""

EASY_TASK = {
    "id": "easy",
    "name": "Customer Lookup",
    "description": "Easy - Single-table filtering with NULL email trap",
    "question": (
        "Find all premium tier customers who signed up after January 1st 2023. "
        "Return their name, email (show 'N/A' if email is missing), and signup_date. "
        "Order alphabetically by name."
    ),
    "expected_sql": (
        "SELECT name, COALESCE(email, 'N/A') AS email, signup_date FROM customers "
        "WHERE tier = 'premium' AND signup_date > '2023-01-01' "
        "ORDER BY name"
    ),
    "expected_columns": ["name", "email", "signup_date"],
    "order_matters": True,
    "max_steps": 12,
    "max_submissions": 3,
    "relevant_tables": ["customers"],
}
