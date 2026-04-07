"""SQLite database setup and seeding with deterministic sample data."""

import sqlite3
from typing import Tuple


def create_database() -> sqlite3.Connection:
    """Create an in-memory SQLite database with all tables and seed data."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON")
    _create_tables(conn)
    _seed_data(conn)
    conn.commit()
    return conn


def _create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            signup_date DATE NOT NULL,
            tier TEXT NOT NULL CHECK (tier IN ('basic', 'premium', 'enterprise')),
            country TEXT NOT NULL
        );

        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL
        );

        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(id),
            order_date DATE NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('completed', 'pending', 'cancelled'))
        );

        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL REFERENCES orders(id),
            product_id INTEGER NOT NULL REFERENCES products(id),
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL
        );
        """
    )


def _seed_data(conn: sqlite3.Connection) -> None:
    # Customers
    customers = [
        (1, "Alice Chen", "alice@example.com", "2022-03-15", "premium", "USA"),
        (2, "Bob Martinez", "bob@example.com", "2023-01-20", "basic", "USA"),
        (3, "Carol White", "carol@example.com", "2023-06-10", "premium", "Canada"),
        (4, "David Kim", "david@example.com", "2021-11-05", "enterprise", "USA"),
        (5, "Eva Muller", "eva@example.com", "2023-03-22", "premium", "Germany"),
        (6, "Frank Tanaka", None, "2022-08-14", "basic", "Japan"),  # NULL email - dirty data trap
        (7, "Grace Liu", "grace@example.com", "2023-09-01", "premium", "Canada"),
        (8, "Henry Patel", "henry@example.com", "2023-04-18", "enterprise", "USA"),
        (9, "Iris Johansson", None, "2022-12-30", "basic", "Germany"),  # NULL email - dirty data trap
        (10, "James Brown", "james@example.com", "2023-07-25", "premium", "USA"),
        (11, "Karen Suzuki", "karen@example.com", "2023-02-14", "basic", "Japan"),
        (12, "Leo Costa", "leo@example.com", "2024-01-10", "premium", "Canada"),
    ]
    conn.executemany(
        "INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?)", customers
    )

    # Products
    products = [
        (1, "Laptop Pro", "Electronics", 1299.99),
        (2, "Wireless Mouse", "Electronics", 29.99),
        (3, "Standing Desk", "Furniture", 499.99),
        (4, "Ergonomic Chair", "Furniture", 349.99),
        (5, "Python Cookbook", "Books", 45.99),
        (6, "Data Science Handbook", "Books", 39.99),
        (7, "Monitor 4K", "Electronics", 599.99),
        (8, "Keyboard Mechanical", "Electronics", 89.99),
        (9, "Desk Lamp", "Furniture", 59.99),
        (10, "SQL Deep Dive", "Books", 52.99),
    ]
    conn.executemany("INSERT INTO products VALUES (?, ?, ?, ?)", products)

    # Orders (mix of dates and statuses — cancelled/pending are traps)
    orders = [
        (1, 1, "2023-02-10", "completed"),
        (2, 2, "2023-05-15", "completed"),
        (3, 3, "2023-10-05", "completed"),
        (4, 4, "2023-10-18", "completed"),
        (5, 5, "2023-11-02", "completed"),
        (6, 1, "2023-11-20", "completed"),
        (7, 6, "2023-12-01", "completed"),
        (8, 7, "2023-12-10", "completed"),
        (9, 8, "2023-12-15", "completed"),
        (10, 2, "2023-10-22", "completed"),
        (11, 10, "2023-11-30", "completed"),
        (12, 4, "2023-12-20", "completed"),
        (13, 3, "2023-11-15", "completed"),
        (14, 9, "2023-12-28", "completed"),
        (15, 5, "2023-10-10", "completed"),
        (16, 11, "2023-12-05", "completed"),
        (17, 12, "2024-01-15", "completed"),
        (18, 10, "2023-12-18", "completed"),
        (19, 8, "2023-11-08", "completed"),
        (20, 1, "2023-12-25", "completed"),
        # TRAPS: cancelled and pending orders — must be excluded from revenue calculations
        (21, 2, "2023-11-10", "cancelled"),   # Bob cancelled a big order
        (22, 4, "2023-12-01", "pending"),      # David has a pending order
        (23, 10, "2023-10-15", "cancelled"),   # James cancelled
        (24, 1, "2023-12-30", "pending"),      # Alice has a pending order
    ]
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", orders)

    # Order items
    order_items = [
        # Order 1 (Alice, Feb 2023)
        (1, 1, 1, 1, 1299.99),
        (2, 1, 2, 2, 29.99),
        # Order 2 (Bob, May 2023)
        (3, 2, 5, 1, 45.99),
        (4, 2, 6, 1, 39.99),
        # Order 3 (Carol, Oct 2023 - Q4)
        (5, 3, 3, 1, 499.99),
        (6, 3, 9, 2, 59.99),
        # Order 4 (David, Oct 2023 - Q4)
        (7, 4, 1, 2, 1299.99),
        (8, 4, 7, 1, 599.99),
        # Order 5 (Eva, Nov 2023 - Q4)
        (9, 5, 4, 1, 349.99),
        (10, 5, 8, 1, 89.99),
        # Order 6 (Alice, Nov 2023 - Q4)
        (11, 6, 7, 1, 599.99),
        (12, 6, 2, 3, 29.99),
        # Order 7 (Frank, Dec 2023 - Q4)
        (13, 7, 5, 2, 45.99),
        (14, 7, 10, 1, 52.99),
        # Order 8 (Grace, Dec 2023 - Q4)
        (15, 8, 3, 1, 499.99),
        (16, 8, 4, 1, 349.99),
        # Order 9 (Henry, Dec 2023 - Q4)
        (17, 9, 1, 1, 1299.99),
        (18, 9, 8, 2, 89.99),
        # Order 10 (Bob, Oct 2023 - Q4)
        (19, 10, 7, 1, 599.99),
        (20, 10, 2, 1, 29.99),
        # Order 11 (James, Nov 2023 - Q4)
        (21, 11, 6, 2, 39.99),
        (22, 11, 10, 1, 52.99),
        # Order 12 (David, Dec 2023 - Q4)
        (23, 12, 4, 1, 349.99),
        (24, 12, 9, 1, 59.99),
        # Order 13 (Carol, Nov 2023 - Q4)
        (25, 13, 8, 1, 89.99),
        (26, 13, 5, 1, 45.99),
        # Order 14 (Iris, Dec 2023 - Q4)
        (27, 14, 6, 1, 39.99),
        (28, 14, 10, 2, 52.99),
        # Order 15 (Eva, Oct 2023 - Q4)
        (29, 15, 3, 1, 499.99),
        # Order 16 (Karen, Dec 2023 - Q4)
        (30, 16, 2, 2, 29.99),
        (31, 16, 5, 1, 45.99),
        # Order 17 (Leo, Jan 2024)
        (32, 17, 1, 1, 1299.99),
        # Order 18 (James, Dec 2023 - Q4)
        (33, 18, 1, 1, 1299.99),
        (34, 18, 3, 1, 499.99),
        # Order 19 (Henry, Nov 2023 - Q4)
        (35, 19, 7, 2, 599.99),
        (36, 19, 5, 1, 45.99),
        # Order 20 (Alice, Dec 2023 - Q4)
        (37, 20, 4, 1, 349.99),
        (38, 20, 10, 1, 52.99),
        # TRAP order items: cancelled/pending orders with HIGH values
        # If agent doesn't filter by status='completed', these will corrupt results
        (39, 21, 1, 3, 1299.99),   # Bob's cancelled order: 3 laptops = $3899.97
        (40, 22, 7, 2, 599.99),    # David's pending order: 2 monitors = $1199.98
        (41, 23, 1, 2, 1299.99),   # James's cancelled order: 2 laptops = $2599.98
        (42, 24, 3, 2, 499.99),    # Alice's pending order: 2 desks = $999.98
    ]
    conn.executemany("INSERT INTO order_items VALUES (?, ?, ?, ?, ?)", order_items)


def get_schema_text(conn: sqlite3.Connection) -> str:
    """Return a human-readable schema description."""
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]

    lines = []
    for table in tables:
        cursor = conn.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        lines.append(f"TABLE: {table}")
        for col in columns:
            # col: (cid, name, type, notnull, default, pk)
            pk = " PRIMARY KEY" if col[5] else ""
            nullable = "" if col[3] else " (nullable)"
            lines.append(f"  - {col[1]} {col[2]}{pk}{nullable}")
        lines.append("")

    return "\n".join(lines).strip()


def get_data_sample(conn: sqlite3.Connection) -> str:
    """Return formatted first 5 rows of each table."""
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]

    sections = []
    for table in tables:
        cursor = conn.execute(f"SELECT * FROM {table} LIMIT 5")
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        header = " | ".join(col_names)
        separator = "-" * len(header)
        data_lines = []
        for row in rows:
            data_lines.append(" | ".join(str(v) for v in row))

        section = f"TABLE: {table}\n{header}\n{separator}\n" + "\n".join(data_lines)
        sections.append(section)

    return "\n\n".join(sections)
