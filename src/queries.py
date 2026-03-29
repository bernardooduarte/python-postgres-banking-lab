from src.database import get_connection, get_dict_cursor


def get_customer_with_accounts(conn, customer_id: str) -> dict:
    cursor = get_dict_cursor(conn)
    cursor.execute("""
        SELECT
            c.id AS customer_id,
            c.full_name,
            c.cpf,
            c.email,
            a.id AS account_id,
            a.account_type,
            a.balance,
            a.status
        FROM customers c
        LEFT JOIN accounts a ON a.customer_id = c.id
        WHERE c.id = %s
        ORDER BY a.created_at;
    """, (customer_id,))
    rows = cursor.fetchall()
    if not rows:
        return None
    result = {
        "customer_id": str(rows[0]["customer_id"]),
        "full_name": rows[0]["full_name"],
        "cpf": rows[0]["cpf"],
        "email": rows[0]["email"],
        "accounts": [
            {
                "account_id": str(row["account_id"]),
                "account_type": row["account_type"],
                "balance": float(row["balance"]),
                "status": row["status"],
            }
            for row in rows if row["account_id"] is not None
        ],
    }
    return result


def get_account_statement(conn, account_id: str, limit: int = 20) -> list:
    cursor = get_dict_cursor(conn)
    cursor.execute("""
        SELECT
            t.id,
            t.type,
            t.amount,
            t.description,
            t.created_at
        FROM transactions t
        WHERE t.account_id = %s
        ORDER BY t.created_at DESC
        LIMIT %s;
    """, (account_id, limit))
    rows = cursor.fetchall()
    return [
        {
            "transaction_id": str(row["id"]),
            "type": row["type"],
            "amount": float(row["amount"]),
            "description": row["description"],
            "created_at": row["created_at"].isoformat(),
        }
        for row in rows
    ]


def get_account_summary(conn, account_id: str) -> dict:
    cursor = get_dict_cursor(conn)
    cursor.execute("""
        SELECT
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN type = 'deposit' THEN amount ELSE 0 END) AS total_deposited,
            SUM(CASE WHEN type = 'withdrawal' THEN amount ELSE 0 END) AS total_withdrawn,
            SUM(CASE WHEN type = 'transfer_in' THEN amount ELSE 0 END) AS total_received,
            SUM(CASE WHEN type = 'transfer_out' THEN amount ELSE 0 END) AS total_sent
        FROM transactions
        WHERE account_id = %s;
    """, (account_id,))
    row = cursor.fetchone()
    return {
        "total_transactions": row["total_transactions"],
        "total_deposited": float(row["total_deposited"] or 0),
        "total_withdrawn": float(row["total_withdrawn"] or 0),
        "total_received": float(row["total_received"] or 0),
        "total_sent": float(row["total_sent"] or 0),
    }


def get_top_accounts_by_balance(conn, limit: int = 5) -> list:
    cursor = get_dict_cursor(conn)
    cursor.execute("""
        SELECT
            a.id AS account_id,
            c.full_name,
            a.account_type,
            a.balance
        FROM accounts a
        JOIN customers c ON c.id = a.customer_id
        WHERE a.status = 'active'
        ORDER BY a.balance DESC
        LIMIT %s;
    """, (limit,))
    rows = cursor.fetchall()
    return [
        {
            "account_id": str(row["account_id"]),
            "full_name": row["full_name"],
            "account_type": row["account_type"],
            "balance": float(row["balance"]),
        }
        for row in rows
    ]


def get_transactions_by_period(conn, account_id: str, start_date: str, end_date: str) -> list:
    cursor = get_dict_cursor(conn)
    cursor.execute("""
        SELECT
            t.id,
            t.type,
            t.amount,
            t.description,
            t.created_at
        FROM transactions t
        WHERE t.account_id = %s
          AND t.created_at >= %s::timestamp
          AND t.created_at < %s::timestamp + INTERVAL '1 day'
        ORDER BY t.created_at;
    """, (account_id, start_date, end_date))
    rows = cursor.fetchall()
    return [
        {
            "transaction_id": str(row["id"]),
            "type": row["type"],
            "amount": float(row["amount"]),
            "description": row["description"],
            "created_at": row["created_at"].isoformat(),
        }
        for row in rows
    ]