import pytest
from src.database import get_connection
from src.operations import create_customer, create_account, deposit, withdraw, transfer
from src.reports import (
    get_running_balance,
    get_monthly_summary,
    get_transaction_ranking,
    get_customer_portfolio,
    get_largest_transactions_per_type,
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
    return create_customer(conn, "Carlos Souza", "55555555555", "carlos@test.com")


@pytest.fixture
def account(conn, customer):
    return create_account(conn, customer, "checking")


def test_running_balance_increases_on_deposit(conn, account):
    deposit(conn, account, 100.00, "First")
    deposit(conn, account, 200.00, "Second")
    deposit(conn, account, 300.00, "Third")

    rows = get_running_balance(conn, account)

    assert len(rows) == 3
    assert rows[-1]["running_balance"] == 600.00
    balances = [r["running_balance"] for r in rows]
    assert balances == sorted(balances)
    
def test_running_balance_decreases_on_withdrawal(conn, account):
    deposit(conn, account, 500.00, "Deposit")
    withdraw(conn, account, 200.00, "Withdrawal")

    rows = get_running_balance(conn, account)

    assert len(rows) == 2
    assert rows[0]["running_balance"] == 500.00
    assert rows[1]["running_balance"] == 300.00


def test_running_balance_with_transfer(conn, customer, account):
    account_b = create_account(conn, customer, "savings")

    deposit(conn, account, 1000.00, "Initial")
    transfer(conn, account, account_b, 400.00, "Transfer")

    rows_a = get_running_balance(conn, account)
    rows_b = get_running_balance(conn, account_b)

    assert rows_a[-1]["running_balance"] == 600.00
    assert rows_b[-1]["running_balance"] == 400.00


def test_monthly_summary_single_month(conn, account):
    deposit(conn, account, 1000.00, "Deposit 1")
    deposit(conn, account, 500.00, "Deposit 2")
    withdraw(conn, account, 300.00, "Withdrawal")

    summary = get_monthly_summary(conn, account)

    assert len(summary) == 1
    month = summary[0]
    assert month["total_transactions"] == 3
    assert month["total_in"] == 1500.00
    assert month["total_out"] == 300.00
    assert month["net"] == 1200.00
    assert month["cumulative_net"] == 1200.00


def test_monthly_summary_cumulative_net(conn, account):
    deposit(conn, account, 1000.00, "Deposit")
    withdraw(conn, account, 200.00, "Withdrawal")

    summary = get_monthly_summary(conn, account)

    assert len(summary) >= 1
    assert summary[-1]["cumulative_net"] == summary[-1]["net"]


def test_transaction_ranking(conn, account):
    deposit(conn, account, 500.00, "Big deposit")
    deposit(conn, account, 100.00, "Small deposit")
    deposit(conn, account, 300.00, "Medium deposit")
    withdraw(conn, account, 200.00, "Withdrawal")

    rows = get_transaction_ranking(conn, account)

    deposits = [r for r in rows if r["type"] == "deposit"]
    assert deposits[0]["amount"] == 500.00
    assert deposits[0]["rank_within_type"] == 1
    assert deposits[1]["amount"] == 300.00
    assert deposits[1]["rank_within_type"] == 2

    total_deposit = sum(r["amount"] for r in deposits)
    pct_sum = sum(r["pct_of_type"] for r in deposits)
    assert abs(pct_sum - 100.00) < 0.1


def test_customer_portfolio(conn, customer):
    account_a = create_account(conn, customer, "checking")
    account_b = create_account(conn, customer, "savings")

    deposit(conn, account_a, 3000.00)
    deposit(conn, account_b, 1000.00)

    portfolio = get_customer_portfolio(conn, customer)

    assert portfolio["total_balance"] == 4000.00
    assert len(portfolio["accounts"]) == 2
    assert portfolio["accounts"][0]["balance"] == 3000.00
    assert portfolio["accounts"][0]["balance_rank"] == 1
    assert portfolio["accounts"][0]["pct_of_total"] == 75.00
    assert portfolio["accounts"][1]["balance"] == 1000.00
    assert portfolio["accounts"][1]["pct_of_total"] == 25.00


def test_largest_transactions_per_type(conn, account):
    deposit(conn, account, 500.00, "Deposit A")
    deposit(conn, account, 100.00, "Deposit B")
    deposit(conn, account, 300.00, "Deposit C")
    withdraw(conn, account, 400.00, "Withdrawal A")
    withdraw(conn, account, 50.00, "Withdrawal B")

    rows = get_largest_transactions_per_type(conn, account, top_n=2)

    deposits = [r for r in rows if r["type"] == "deposit"]
    withdrawals = [r for r in rows if r["type"] == "withdrawal"]

    assert len(deposits) == 2
    assert len(withdrawals) == 2
    assert deposits[0]["amount"] == 500.00
    assert deposits[0]["rank_within_type"] == 1
    assert withdrawals[0]["amount"] == 400.00
    assert withdrawals[0]["rank_within_type"] == 1


def test_largest_transactions_respects_top_n(conn, account):
    for amount in [100, 200, 300, 400, 500]:
        deposit(conn, account, float(amount))

    rows = get_largest_transactions_per_type(conn, account, top_n=3)
    deposits = [r for r in rows if r["type"] == "deposit"]

    assert len(deposits) == 3
    assert deposits[0]["amount"] == 500.00