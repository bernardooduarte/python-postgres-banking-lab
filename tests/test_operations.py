import pytest
import psycopg2
from src.database import get_connection
from src.operations import create_customer, create_account, deposit, withdraw, transfer


@pytest.fixture(scope="session")
def conn():
    connection = get_connection()
    yield connection
    connection.rollback()
    connection.close()


@pytest.fixture(autouse=True)
def rollback_each_test(conn):
    cursor = conn.cursor()
    cursor.execute("SAVEPOINT test_start")
    yield
    cursor.execute("ROLLBACK TO SAVEPOINT test_start")
    cursor.close()

@pytest.fixture
def customer(conn):
    return create_customer(conn, "John Doe", "11111111111", "john@test.com")


@pytest.fixture
def account(conn, customer):
    return create_account(conn, customer, "checking")


def test_create_customer(conn):
    customer_id = create_customer(conn, "Jane Doe", "22222222222", "jane@test.com")
    assert customer_id is not None
    cursor = conn.cursor()
    cursor.execute("SELECT full_name FROM customers WHERE id = %s;", (customer_id,))
    assert cursor.fetchone()[0] == "Jane Doe"


def test_create_account(conn, customer):
    account_id = create_account(conn, customer, "savings")
    assert account_id is not None
    cursor = conn.cursor()
    cursor.execute("SELECT account_type, balance FROM accounts WHERE id = %s;", (account_id,))
    row = cursor.fetchone()
    assert row[0] == "savings"
    assert row[1] == 0


def test_deposit(conn, account):
    deposit(conn, account, 500.00, "Initial deposit")
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM accounts WHERE id = %s;", (account,))
    assert cursor.fetchone()[0] == 500.00


def test_deposit_invalid_amount(conn, account):
    with pytest.raises(ValueError, match="must be positive"):
        deposit(conn, account, -100.00)


def test_withdraw(conn, account):
    deposit(conn, account, 1000.00)
    withdraw(conn, account, 300.00, "ATM withdrawal")
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM accounts WHERE id = %s;", (account,))
    assert cursor.fetchone()[0] == 700.00


def test_withdraw_insufficient_funds(conn, account):
    deposit(conn, account, 100.00)
    with pytest.raises(ValueError, match="Insufficient funds"):
        withdraw(conn, account, 500.00)


def test_withdraw_invalid_amount(conn, account):
    with pytest.raises(ValueError, match="must be positive"):
        withdraw(conn, account, 0)


def test_transfer(conn, customer):
    account_a = create_account(conn, customer, "checking")
    account_b = create_account(conn, customer, "savings")
    deposit(conn, account_a, 1000.00)
    transfer(conn, account_a, account_b, 400.00, "Transfer test")
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM accounts WHERE id = %s;", (account_a,))
    assert cursor.fetchone()[0] == 600.00
    cursor.execute("SELECT balance FROM accounts WHERE id = %s;", (account_b,))
    assert cursor.fetchone()[0] == 400.00


def test_transfer_insufficient_funds(conn, customer):
    account_a = create_account(conn, customer, "checking")
    account_b = create_account(conn, customer, "savings")
    deposit(conn, account_a, 100.00)
    with pytest.raises(ValueError, match="Insufficient funds"):
        transfer(conn, account_a, account_b, 500.00)


def test_transaction_history_recorded(conn, account):
    deposit(conn, account, 200.00, "Deposit 1")
    deposit(conn, account, 300.00, "Deposit 2")
    withdraw(conn, account, 100.00, "Withdrawal 1")
    cursor = conn.cursor()
    cursor.execute("SELECT type, amount FROM transactions WHERE account_id = %s ORDER BY created_at;", (account,))
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0] == ("deposit", 200.00)
    assert rows[1] == ("deposit", 300.00)
    assert rows[2] == ("withdrawal", 100.00)