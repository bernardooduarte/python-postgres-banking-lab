import pytest
from src.database import get_connection
from src.operations import create_customer, create_account, deposit, withdraw, transfer
from src.queries import (
    get_customer_with_accounts,
    get_account_statement,
    get_account_summary,
    get_top_accounts_by_balance,
    get_transactions_by_period,
)


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
    return create_customer(conn, "Alice Silva", "33333333333", "alice@test.com")


@pytest.fixture
def account(conn, customer):
    return create_account(conn, customer, "checking")


def test_get_customer_with_accounts(conn, customer):
    account_a = create_account(conn, customer, "checking")
    account_b = create_account(conn, customer, "savings")

    result = get_customer_with_accounts(conn, customer)

    assert result["full_name"] == "Alice Silva"
    assert len(result["accounts"]) == 2
    account_types = [a["account_type"] for a in result["accounts"]]
    assert "checking" in account_types
    assert "savings" in account_types


def test_get_customer_with_no_accounts(conn):
    customer_id = create_customer(conn, "Bob Lima", "44444444444", "bob@test.com")
    result = get_customer_with_accounts(conn, customer_id)
    assert result["full_name"] == "Bob Lima"
    assert result["accounts"] == []


def test_get_customer_not_found(conn):
    result = get_customer_with_accounts(conn, "00000000-0000-0000-0000-000000000000")
    assert result is None


def test_get_account_statement(conn, account):
    deposit(conn, account, 100.00, "First deposit")
    deposit(conn, account, 200.00, "Second deposit")
    withdraw(conn, account, 50.00, "Withdrawal")

    statement = get_account_statement(conn, account)

    assert len(statement) == 3
    types = [s["type"] for s in statement]
    amounts = [s["amount"] for s in statement]
    assert "withdrawal" in types
    assert "deposit" in types
    assert 50.00 in amounts
    assert 100.00 in amounts
    assert 200.00 in amounts

def test_get_account_statement_limit(conn, account):
    for i in range(10):
        deposit(conn, account, 10.00, f"Deposit {i}")

    statement = get_account_statement(conn, account, limit=5)
    assert len(statement) == 5


def test_get_account_summary(conn, customer, account):
    deposit(conn, account, 1000.00, "Deposit")
    withdraw(conn, account, 200.00, "Withdrawal")

    account_b = create_account(conn, customer, "savings")
    transfer(conn, account, account_b, 300.00, "Transfer")

    summary = get_account_summary(conn, account)

    assert summary["total_transactions"] == 3
    assert summary["total_deposited"] == 1000.00
    assert summary["total_withdrawn"] == 200.00
    assert summary["total_sent"] == 300.00
    assert summary["total_received"] == 0.0

def test_get_top_accounts_by_balance(conn, customer):
    account_a = create_account(conn, customer, "checking")
    account_b = create_account(conn, customer, "savings")
    account_c = create_account(conn, customer, "checking")

    deposit(conn, account_a, 5000.00)
    deposit(conn, account_b, 3000.00)
    deposit(conn, account_c, 1000.00)

    top = get_top_accounts_by_balance(conn, limit=3)

    assert len(top) <= 3
    balances = [t["balance"] for t in top]
    assert balances == sorted(balances, reverse=True)


def test_get_transactions_by_period(conn, account):
    deposit(conn, account, 100.00, "Morning deposit")
    deposit(conn, account, 200.00, "Afternoon deposit")
    withdraw(conn, account, 50.00, "Evening withdrawal")

    from datetime import date
    today = date.today().isoformat()

    transactions = get_transactions_by_period(conn, account, today, today)

    assert len(transactions) == 3
    amounts = [t["amount"] for t in transactions]
    assert 100.00 in amounts
    assert 200.00 in amounts
    assert 50.00 in amounts