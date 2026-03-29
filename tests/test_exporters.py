import os
import csv
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
from src.exporters import (
    export_to_csv,
    export_to_csv_string,
    export_to_pdf,
    export_to_pdf_bytes,
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
    return create_customer(conn, "Diana Ferreira", "66666666666", "diana@test.com")


@pytest.fixture
def account(conn, customer):
    acc = create_account(conn, customer, "checking")
    deposit(conn, acc, 1000.00, "Initial deposit")
    deposit(conn, acc, 500.00, "Second deposit")
    withdraw(conn, acc, 200.00, "Withdrawal")
    return acc


@pytest.fixture
def statement_data(conn, account):
    return get_running_balance(conn, account)


# --- CSV ---

def test_export_csv_creates_file(tmp_path, statement_data):
    filepath = str(tmp_path / "statement.csv")
    export_to_csv(statement_data, filepath)
    assert os.path.exists(filepath)


def test_export_csv_correct_row_count(tmp_path, statement_data):
    filepath = str(tmp_path / "statement.csv")
    export_to_csv(statement_data, filepath)
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    assert len(rows) == len(statement_data)


def test_export_csv_correct_headers(tmp_path, statement_data):
    filepath = str(tmp_path / "statement.csv")
    export_to_csv(statement_data, filepath)
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
    expected = list(statement_data[0].keys())
    assert headers == expected


def test_export_csv_correct_values(tmp_path, statement_data):
    filepath = str(tmp_path / "statement.csv")
    export_to_csv(statement_data, filepath)
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    assert rows[-1]["running_balance"] == str(statement_data[-1]["running_balance"])


def test_export_csv_string_returns_string(statement_data):
    result = export_to_csv_string(statement_data)
    assert isinstance(result, str)


def test_export_csv_string_has_header(statement_data):
    result = export_to_csv_string(statement_data)
    first_line = result.splitlines()[0]
    for key in statement_data[0].keys():
        assert key in first_line


def test_export_csv_string_correct_row_count(statement_data):
    result = export_to_csv_string(statement_data)
    lines = [l for l in result.splitlines() if l.strip()]
    assert len(lines) == len(statement_data) + 1  # header + rows


def test_export_csv_raises_on_empty_data():
    with pytest.raises(ValueError, match="No data to export"):
        export_to_csv([], "/tmp/empty.csv")


def test_export_csv_string_raises_on_empty_data():
    with pytest.raises(ValueError, match="No data to export"):
        export_to_csv_string([])


# --- PDF ---

def test_export_pdf_creates_file(tmp_path, statement_data):
    filepath = str(tmp_path / "statement.pdf")
    export_to_pdf(statement_data, filepath, title="Running Balance")
    assert os.path.exists(filepath)


def test_export_pdf_file_not_empty(tmp_path, statement_data):
    filepath = str(tmp_path / "statement.pdf")
    export_to_pdf(statement_data, filepath, title="Running Balance")
    assert os.path.getsize(filepath) > 0


def test_export_pdf_is_valid_pdf(tmp_path, statement_data):
    filepath = str(tmp_path / "statement.pdf")
    export_to_pdf(statement_data, filepath, title="Running Balance")
    with open(filepath, "rb") as f:
        header = f.read(4)
    assert header == b"%PDF"


def test_export_pdf_with_subtitle(tmp_path, statement_data):
    filepath = str(tmp_path / "statement_subtitle.pdf")
    export_to_pdf(
        statement_data,
        filepath,
        title="Running Balance",
        subtitle="Account: 12345",
    )
    assert os.path.exists(filepath)
    assert os.path.getsize(filepath) > 0


def test_export_pdf_bytes_returns_bytes(statement_data):
    result = export_to_pdf_bytes(statement_data, title="Running Balance")
    assert isinstance(result, bytes)


def test_export_pdf_bytes_is_valid_pdf(statement_data):
    result = export_to_pdf_bytes(statement_data, title="Running Balance")
    assert result[:4] == b"%PDF"


def test_export_pdf_bytes_not_empty(statement_data):
    result = export_to_pdf_bytes(statement_data, title="Running Balance")
    assert len(result) > 0


def test_export_pdf_raises_on_empty_data(tmp_path):
    with pytest.raises(ValueError, match="No data to export"):
        export_to_pdf([], str(tmp_path / "empty.pdf"), title="Empty")


def test_export_pdf_bytes_raises_on_empty_data():
    with pytest.raises(ValueError, match="No data to export"):
        export_to_pdf_bytes([], title="Empty")


# --- integração com reports ---

def test_export_monthly_summary_csv(tmp_path, conn, account):
    data = get_monthly_summary(conn, account)
    filepath = str(tmp_path / "monthly.csv")
    export_to_csv(data, filepath)
    assert os.path.exists(filepath)
    with open(filepath, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == len(data)


def test_export_transaction_ranking_pdf(tmp_path, conn, account):
    data = get_transaction_ranking(conn, account)
    filepath = str(tmp_path / "ranking.pdf")
    export_to_pdf(data, filepath, title="Transaction Ranking")
    assert os.path.exists(filepath)
    with open(filepath, "rb") as f:
        assert f.read(4) == b"%PDF"


def test_export_largest_transactions_csv_string(conn, account):
    data = get_largest_transactions_per_type(conn, account, top_n=2)
    result = export_to_csv_string(data)
    assert "type" in result.splitlines()[0]
    assert "amount" in result.splitlines()[0]


def test_export_customer_portfolio_pdf_bytes(conn, customer, account):
    data = get_customer_portfolio(conn, customer)["accounts"]
    result = export_to_pdf_bytes(data, title="Customer Portfolio")
    assert result[:4] == b"%PDF"