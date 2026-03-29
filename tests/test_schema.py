import pytest
import psycopg2
from src.database import get_connection


@pytest.fixture
def conn():
    connection = get_connection()
    yield connection
    connection.rollback()
    connection.close()


def test_customers_table_exists(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'customers'
        );
    """)
    exists = cursor.fetchone()[0]
    assert exists is True


def test_accounts_table_exists(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'accounts'
        );
    """)
    exists = cursor.fetchone()[0]
    assert exists is True


def test_transactions_table_exists(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'transactions'
        );
    """)
    exists = cursor.fetchone()[0]
    assert exists is True


def test_account_balance_cannot_be_negative(conn):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO customers (full_name, cpf, email)
        VALUES ('Test User', '00000000001', 'test@test.com')
        RETURNING id;
    """)
    customer_id = cursor.fetchone()[0]

    with pytest.raises(psycopg2.errors.CheckViolation):
        cursor.execute("""
            INSERT INTO accounts (customer_id, account_type, balance)
            VALUES (%s, 'checking', -100.00);
        """, (customer_id,))


def test_transaction_amount_must_be_positive(conn):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO customers (full_name, cpf, email)
        VALUES ('Test User 2', '00000000002', 'test2@test.com')
        RETURNING id;
    """)
    customer_id = cursor.fetchone()[0]

    cursor.execute("""
        INSERT INTO accounts (customer_id, account_type)
        VALUES (%s, 'savings')
        RETURNING id;
    """, (customer_id,))
    account_id = cursor.fetchone()[0]

    with pytest.raises(psycopg2.errors.CheckViolation):
        cursor.execute("""
            INSERT INTO transactions (account_id, type, amount)
            VALUES (%s, 'deposit', -50.00);
        """, (account_id,))