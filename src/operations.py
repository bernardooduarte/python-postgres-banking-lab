import psycopg2
from src.database import get_connection


def create_customer(conn, full_name: str, cpf: str, email: str) -> str:
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO customers (full_name, cpf, email)
        VALUES (%s, %s, %s)
        RETURNING id;
    """, (full_name, cpf, email))
    return str(cursor.fetchone()[0])


def create_account(conn, customer_id: str, account_type: str) -> str:
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO accounts (customer_id, account_type)
        VALUES (%s, %s)
        RETURNING id;
    """, (customer_id, account_type))
    return str(cursor.fetchone()[0])


def deposit(conn, account_id: str, amount: float, description: str = None) -> None:
    if amount <= 0:
        raise ValueError("Deposit amount must be positive.")
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE accounts SET balance = balance + %s
        WHERE id = %s AND status = 'active';
    """, (amount, account_id))
    if cursor.rowcount == 0:
        raise ValueError("Account not found or inactive.")
    cursor.execute("""
        INSERT INTO transactions (account_id, type, amount, description)
        VALUES (%s, 'deposit', %s, %s);
    """, (account_id, amount, description))


def withdraw(conn, account_id: str, amount: float, description: str = None) -> None:
    if amount <= 0:
        raise ValueError("Withdrawal amount must be positive.")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT balance FROM accounts
        WHERE id = %s AND status = 'active';
    """, (account_id,))
    row = cursor.fetchone()
    if row is None:
        raise ValueError("Account not found or inactive.")
    if row[0] < amount:
        raise ValueError("Insufficient funds.")
    cursor.execute("""
        UPDATE accounts SET balance = balance - %s
        WHERE id = %s;
    """, (amount, account_id))
    cursor.execute("""
        INSERT INTO transactions (account_id, type, amount, description)
        VALUES (%s, 'withdrawal', %s, %s);
    """, (account_id, amount, description))


def transfer(conn, from_account_id: str, to_account_id: str, amount: float, description: str = None) -> None:
    if amount <= 0:
        raise ValueError("Transfer amount must be positive.")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT balance FROM accounts
        WHERE id = %s AND status = 'active';
    """, (from_account_id,))
    row = cursor.fetchone()
    if row is None:
        raise ValueError("Source account not found or inactive.")
    if row[0] < amount:
        raise ValueError("Insufficient funds.")
    cursor.execute("""
        SELECT id FROM accounts
        WHERE id = %s AND status = 'active';
    """, (to_account_id,))
    if cursor.fetchone() is None:
        raise ValueError("Destination account not found or inactive.")
    cursor.execute("""
        UPDATE accounts SET balance = balance - %s WHERE id = %s;
    """, (amount, from_account_id))
    cursor.execute("""
        UPDATE accounts SET balance = balance + %s WHERE id = %s;
    """, (amount, to_account_id))
    cursor.execute("""
        INSERT INTO transactions (account_id, type, amount, description)
        VALUES (%s, 'transfer_out', %s, %s);
    """, (from_account_id, amount, description))
    cursor.execute("""
        INSERT INTO transactions (account_id, type, amount, description)
        VALUES (%s, 'transfer_in', %s, %s);
    """, (to_account_id, amount, description))