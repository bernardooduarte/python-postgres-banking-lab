import pytest
from fastapi.testclient import TestClient
from src.api import app
from src.database import get_connection
from src.operations import create_customer, create_account, deposit, withdraw, transfer
import src.api as api_module


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


@pytest.fixture(scope="session")
def client(conn):
    api_module._override_conn = conn
    with TestClient(app) as c:
        yield c
    api_module._override_conn = None

@pytest.fixture
def customer(conn):
    return create_customer(conn, "Elena Costa", "77777777777", "elena@test.com")


@pytest.fixture
def account(conn, customer):
    acc = create_account(conn, customer, "checking")
    deposit(conn, acc, 1000.00, "Initial deposit")
    deposit(conn, acc, 500.00, "Second deposit")
    withdraw(conn, acc, 200.00, "Withdrawal")
    return acc


# --- customers ---

def test_customer_detail_found(client, customer):
    response = client.get(f"/customers/{customer}")
    assert response.status_code == 200
    data = response.json()
    assert data["full_name"] == "Elena Costa"
    assert "accounts" in data


def test_customer_detail_not_found(client):
    response = client.get("/customers/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404


def test_customer_portfolio(client, customer, account):
    response = client.get(f"/customers/{customer}/portfolio")
    assert response.status_code == 200
    data = response.json()
    assert "total_balance" in data
    assert "accounts" in data
    assert len(data["accounts"]) == 1


def test_customer_portfolio_csv(client, customer, account):
    response = client.get(f"/customers/{customer}/portfolio/export/csv")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv; charset=utf-8"
    lines = response.text.splitlines()
    assert len(lines) >= 2
    assert "account_id" in lines[0]


def test_customer_portfolio_pdf(client, customer, account):
    response = client.get(f"/customers/{customer}/portfolio/export/pdf")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/pdf"
    assert response.content[:4] == b"%PDF"


# --- accounts ---

def test_account_statement(client, account):
    response = client.get(f"/accounts/{account}/statement")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 3


def test_account_statement_with_limit(client, account):
    response = client.get(f"/accounts/{account}/statement?limit=2")
    assert response.status_code == 200
    assert len(response.json()) == 2


def test_account_statement_limit_validation(client, account):
    response = client.get(f"/accounts/{account}/statement?limit=0")
    assert response.status_code == 422


def test_account_summary(client, account):
    response = client.get(f"/accounts/{account}/summary")
    assert response.status_code == 200
    data = response.json()
    assert "total_transactions" in data
    assert data["total_deposited"] == 1500.00
    assert data["total_withdrawn"] == 200.00


def test_account_transactions_by_period(client, account):
    from datetime import date
    today = date.today().isoformat()
    response = client.get(
        f"/accounts/{account}/transactions?start_date={today}&end_date={today}"
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 3


def test_account_transactions_missing_params(client, account):
    response = client.get(f"/accounts/{account}/transactions")
    assert response.status_code == 422


# --- reports ---

def test_report_running_balance(client, account):
    response = client.get(f"/accounts/{account}/reports/running-balance")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 3
    assert data[-1]["running_balance"] == 1300.00


def test_report_running_balance_csv(client, account):
    response = client.get(f"/accounts/{account}/reports/running-balance/export/csv")
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    lines = response.text.splitlines()
    assert "running_balance" in lines[0]
    assert len(lines) == 4


def test_report_running_balance_pdf(client, account):
    response = client.get(f"/accounts/{account}/reports/running-balance/export/pdf")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/pdf"
    assert response.content[:4] == b"%PDF"


def test_report_monthly_summary(client, account):
    response = client.get(f"/accounts/{account}/reports/monthly-summary")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    assert "total_in" in data[0]
    assert "cumulative_net" in data[0]


def test_report_monthly_summary_csv(client, account):
    response = client.get(f"/accounts/{account}/reports/monthly-summary/export/csv")
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert "month" in response.text.splitlines()[0]


def test_report_monthly_summary_pdf(client, account):
    response = client.get(f"/accounts/{account}/reports/monthly-summary/export/pdf")
    assert response.status_code == 200
    assert response.content[:4] == b"%PDF"


def test_report_transaction_ranking(client, account):
    response = client.get(f"/accounts/{account}/reports/transaction-ranking")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert "rank_within_type" in data[0]
    assert "pct_of_type" in data[0]


def test_report_transaction_ranking_csv(client, account):
    response = client.get(f"/accounts/{account}/reports/transaction-ranking/export/csv")
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert "rank_within_type" in response.text.splitlines()[0]


def test_report_transaction_ranking_pdf(client, account):
    response = client.get(f"/accounts/{account}/reports/transaction-ranking/export/pdf")
    assert response.status_code == 200
    assert response.content[:4] == b"%PDF"


def test_report_largest_transactions(client, account):
    response = client.get(f"/accounts/{account}/reports/largest-transactions?top_n=2")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    deposits = [r for r in data if r["type"] == "deposit"]
    assert len(deposits) == 2
    assert deposits[0]["rank_within_type"] == 1


def test_report_largest_transactions_csv(client, account):
    response = client.get(f"/accounts/{account}/reports/largest-transactions/export/csv?top_n=2")
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]


def test_report_largest_transactions_pdf(client, account):
    response = client.get(f"/accounts/{account}/reports/largest-transactions/export/pdf?top_n=2")
    assert response.status_code == 200
    assert response.content[:4] == b"%PDF"


def test_report_largest_transactions_top_n_validation(client, account):
    response = client.get(f"/accounts/{account}/reports/largest-transactions?top_n=0")
    assert response.status_code == 422


# --- top accounts ---

def test_top_accounts_by_balance(client, customer, account):
    response = client.get("/accounts/top/by-balance?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    balances = [r["balance"] for r in data]
    assert balances == sorted(balances, reverse=True)


def test_top_accounts_limit_validation(client):
    response = client.get("/accounts/top/by-balance?limit=0")
    assert response.status_code == 422