from src.database import get_dict_cursor


def get_running_balance(conn, account_id: str) -> list:
    cursor = get_dict_cursor(conn)
    cursor.execute("""
        SELECT
            t.id,
            t.type,
            t.amount,
            t.description,
            t.created_at,
            SUM(
                CASE
                    WHEN t.type IN ('deposit', 'transfer_in') THEN t.amount
                    ELSE -t.amount
                END
            ) OVER (ORDER BY t.created_at, t.id) AS running_balance
        FROM transactions t
        WHERE t.account_id = %s
        ORDER BY t.created_at, t.id;
    """, (account_id,))
    rows = cursor.fetchall()
    return [
        {
            "transaction_id": str(row["id"]),
            "type": row["type"],
            "amount": float(row["amount"]),
            "description": row["description"],
            "created_at": row["created_at"].isoformat(),
            "running_balance": float(row["running_balance"]),
        }
        for row in rows
    ]


def get_monthly_summary(conn, account_id: str) -> list:
    cursor = get_dict_cursor(conn)
    cursor.execute("""
        SELECT
            TO_CHAR(DATE_TRUNC('month', created_at), 'YYYY-MM') AS month,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN type IN ('deposit', 'transfer_in') THEN amount ELSE 0 END) AS total_in,
            SUM(CASE WHEN type IN ('withdrawal', 'transfer_out') THEN amount ELSE 0 END) AS total_out,
            SUM(CASE WHEN type IN ('deposit', 'transfer_in') THEN amount ELSE 0 END) -
            SUM(CASE WHEN type IN ('withdrawal', 'transfer_out') THEN amount ELSE 0 END) AS net,
            SUM(
                SUM(
                    CASE
                        WHEN type IN ('deposit', 'transfer_in') THEN amount
                        ELSE -amount
                    END
                )
            ) OVER (ORDER BY DATE_TRUNC('month', created_at)) AS cumulative_net
        FROM transactions
        WHERE account_id = %s
        GROUP BY DATE_TRUNC('month', created_at)
        ORDER BY DATE_TRUNC('month', created_at);
    """, (account_id,))
    rows = cursor.fetchall()
    return [
        {
            "month": row["month"],
            "total_transactions": row["total_transactions"],
            "total_in": float(row["total_in"]),
            "total_out": float(row["total_out"]),
            "net": float(row["net"]),
            "cumulative_net": float(row["cumulative_net"]),
        }
        for row in rows
    ]


def get_transaction_ranking(conn, account_id: str) -> list:
    cursor = get_dict_cursor(conn)
    cursor.execute("""
        SELECT
            t.id,
            t.type,
            t.amount,
            t.description,
            t.created_at,
            RANK() OVER (PARTITION BY t.type ORDER BY t.amount DESC) AS rank_within_type,
            ROUND(
                t.amount * 100.0 / SUM(t.amount) OVER (PARTITION BY t.type),
                2
            ) AS pct_of_type
        FROM transactions t
        WHERE t.account_id = %s
        ORDER BY t.type, rank_within_type;
    """, (account_id,))
    rows = cursor.fetchall()
    return [
        {
            "transaction_id": str(row["id"]),
            "type": row["type"],
            "amount": float(row["amount"]),
            "description": row["description"],
            "created_at": row["created_at"].isoformat(),
            "rank_within_type": row["rank_within_type"],
            "pct_of_type": float(row["pct_of_type"]),
        }
        for row in rows
    ]


def get_customer_portfolio(conn, customer_id: str) -> dict:
    cursor = get_dict_cursor(conn)
    cursor.execute("""
        SELECT
            a.id AS account_id,
            a.account_type,
            a.balance,
            a.status,
            RANK() OVER (ORDER BY a.balance DESC) AS balance_rank,
            ROUND(
                a.balance * 100.0 / NULLIF(SUM(a.balance) OVER (), 0),
                2
            ) AS pct_of_total,
            COALESCE(t.transaction_count, 0) AS transaction_count,
            SUM(a.balance) OVER () AS total_balance
        FROM accounts a
        LEFT JOIN (
            SELECT account_id, COUNT(*) AS transaction_count
            FROM transactions
            GROUP BY account_id
        ) t ON t.account_id = a.id
        WHERE a.customer_id = %s
        ORDER BY a.balance DESC;
    """, (customer_id,))
    rows = cursor.fetchall()
    if not rows:
        return {"accounts": [], "total_balance": 0.0}
    return {
        "total_balance": float(rows[0]["total_balance"]),
        "accounts": [
            {
                "account_id": str(row["account_id"]),
                "account_type": row["account_type"],
                "balance": float(row["balance"]),
                "status": row["status"],
                "balance_rank": row["balance_rank"],
                "pct_of_total": float(row["pct_of_total"] or 0),
                "transaction_count": row["transaction_count"],
            }
            for row in rows
        ],
    }

def get_largest_transactions_per_type(conn, account_id: str, top_n: int = 3) -> list:
    cursor = get_dict_cursor(conn)
    cursor.execute("""
        SELECT
            transaction_id,
            type,
            amount,
            description,
            created_at,
            rank_within_type
        FROM (
            SELECT
                t.id AS transaction_id,
                t.type,
                t.amount,
                t.description,
                t.created_at,
                ROW_NUMBER() OVER (PARTITION BY t.type ORDER BY t.amount DESC) AS rank_within_type
            FROM transactions t
            WHERE t.account_id = %s
        ) ranked
        WHERE rank_within_type <= %s
        ORDER BY type, rank_within_type;
    """, (account_id, top_n))
    rows = cursor.fetchall()
    return [
        {
            "transaction_id": str(row["transaction_id"]),
            "type": row["type"],
            "amount": float(row["amount"]),
            "description": row["description"],
            "created_at": row["created_at"].isoformat(),
            "rank_within_type": row["rank_within_type"],
        }
        for row in rows
    ]