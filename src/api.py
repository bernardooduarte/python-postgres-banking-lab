from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse
from src.database import get_connection
from src.queries import (
    get_customer_with_accounts,
    get_account_statement,
    get_account_summary,
    get_top_accounts_by_balance,
    get_transactions_by_period,
)
from src.reports import (
    get_running_balance,
    get_monthly_summary,
    get_transaction_ranking,
    get_customer_portfolio,
    get_largest_transactions_per_type,
)
from src.exporters import (
    export_to_csv_string,
    export_to_pdf_bytes,
)
import io


app = FastAPI(title="Banking Lab API", version="1.0.0")

_override_conn = None

def get_conn():
    if _override_conn is not None:
        return _override_conn
    return get_connection()


# --- customers ---

@app.get("/customers/{customer_id}")
def customer_detail(customer_id: str):
    conn = get_conn()
    result = get_customer_with_accounts(conn, customer_id)
    
    if not result:
        raise HTTPException(status_code=404, detail="Customer not found.")
    return result


@app.get("/customers/{customer_id}/portfolio")
def customer_portfolio(customer_id: str):
    conn = get_conn()
    result = get_customer_portfolio(conn, customer_id)
    
    return result


@app.get("/customers/{customer_id}/portfolio/export/csv")
def customer_portfolio_csv(customer_id: str):
    conn = get_conn()
    data = get_customer_portfolio(conn, customer_id)["accounts"]
    
    if not data:
        raise HTTPException(status_code=404, detail="No accounts found.")
    content = export_to_csv_string(data)
    return StreamingResponse(
        io.StringIO(content),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=portfolio_{customer_id}.csv"},
    )


@app.get("/customers/{customer_id}/portfolio/export/pdf")
def customer_portfolio_pdf(customer_id: str):
    conn = get_conn()
    data = get_customer_portfolio(conn, customer_id)["accounts"]
    
    if not data:
        raise HTTPException(status_code=404, detail="No accounts found.")
    content = export_to_pdf_bytes(data, title="Customer Portfolio")
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=portfolio_{customer_id}.pdf"},
    )


# --- accounts ---

@app.get("/accounts/{account_id}/statement")
def account_statement(
    account_id: str,
    limit: int = Query(default=20, ge=1, le=200),
):
    conn = get_conn()
    result = get_account_statement(conn, account_id, limit=limit)
    
    return result


@app.get("/accounts/{account_id}/summary")
def account_summary(account_id: str):
    conn = get_conn()
    result = get_account_summary(conn, account_id)
    
    return result


@app.get("/accounts/{account_id}/transactions")
def account_transactions_by_period(
    account_id: str,
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
):
    conn = get_conn()
    result = get_transactions_by_period(conn, account_id, start_date, end_date)
    
    return result


# --- reports ---

@app.get("/accounts/{account_id}/reports/running-balance")
def report_running_balance(account_id: str):
    conn = get_conn()
    result = get_running_balance(conn, account_id)
    
    return result


@app.get("/accounts/{account_id}/reports/running-balance/export/csv")
def report_running_balance_csv(account_id: str):
    conn = get_conn()
    data = get_running_balance(conn, account_id)
    
    if not data:
        raise HTTPException(status_code=404, detail="No transactions found.")
    content = export_to_csv_string(data)
    return StreamingResponse(
        io.StringIO(content),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=running_balance_{account_id}.csv"},
    )


@app.get("/accounts/{account_id}/reports/running-balance/export/pdf")
def report_running_balance_pdf(account_id: str):
    conn = get_conn()
    data = get_running_balance(conn, account_id)
    
    if not data:
        raise HTTPException(status_code=404, detail="No transactions found.")
    content = export_to_pdf_bytes(data, title="Running Balance Report")
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=running_balance_{account_id}.pdf"},
    )


@app.get("/accounts/{account_id}/reports/monthly-summary")
def report_monthly_summary(account_id: str):
    conn = get_conn()
    result = get_monthly_summary(conn, account_id)
    
    return result


@app.get("/accounts/{account_id}/reports/monthly-summary/export/csv")
def report_monthly_summary_csv(account_id: str):
    conn = get_conn()
    data = get_monthly_summary(conn, account_id)
    
    if not data:
        raise HTTPException(status_code=404, detail="No data found.")
    content = export_to_csv_string(data)
    return StreamingResponse(
        io.StringIO(content),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=monthly_summary_{account_id}.csv"},
    )


@app.get("/accounts/{account_id}/reports/monthly-summary/export/pdf")
def report_monthly_summary_pdf(account_id: str):
    conn = get_conn()
    data = get_monthly_summary(conn, account_id)
    
    if not data:
        raise HTTPException(status_code=404, detail="No data found.")
    content = export_to_pdf_bytes(data, title="Monthly Summary Report")
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=monthly_summary_{account_id}.pdf"},
    )


@app.get("/accounts/{account_id}/reports/transaction-ranking")
def report_transaction_ranking(account_id: str):
    conn = get_conn()
    result = get_transaction_ranking(conn, account_id)
    
    return result


@app.get("/accounts/{account_id}/reports/transaction-ranking/export/csv")
def report_transaction_ranking_csv(account_id: str):
    conn = get_conn()
    data = get_transaction_ranking(conn, account_id)
    
    if not data:
        raise HTTPException(status_code=404, detail="No transactions found.")
    content = export_to_csv_string(data)
    return StreamingResponse(
        io.StringIO(content),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=transaction_ranking_{account_id}.csv"},
    )


@app.get("/accounts/{account_id}/reports/transaction-ranking/export/pdf")
def report_transaction_ranking_pdf(account_id: str):
    conn = get_conn()
    data = get_transaction_ranking(conn, account_id)
    
    if not data:
        raise HTTPException(status_code=404, detail="No transactions found.")
    content = export_to_pdf_bytes(data, title="Transaction Ranking Report")
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=transaction_ranking_{account_id}.pdf"},
    )


@app.get("/accounts/{account_id}/reports/largest-transactions")
def report_largest_transactions(
    account_id: str,
    top_n: int = Query(default=3, ge=1, le=50),
):
    conn = get_conn()
    result = get_largest_transactions_per_type(conn, account_id, top_n=top_n)
    
    return result


@app.get("/accounts/{account_id}/reports/largest-transactions/export/csv")
def report_largest_transactions_csv(
    account_id: str,
    top_n: int = Query(default=3, ge=1, le=50),
):
    conn = get_conn()
    data = get_largest_transactions_per_type(conn, account_id, top_n=top_n)
    
    if not data:
        raise HTTPException(status_code=404, detail="No transactions found.")
    content = export_to_csv_string(data)
    return StreamingResponse(
        io.StringIO(content),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=largest_transactions_{account_id}.csv"},
    )


@app.get("/accounts/{account_id}/reports/largest-transactions/export/pdf")
def report_largest_transactions_pdf(
    account_id: str,
    top_n: int = Query(default=3, ge=1, le=50),
):
    conn = get_conn()
    data = get_largest_transactions_per_type(conn, account_id, top_n=top_n)
    
    if not data:
        raise HTTPException(status_code=404, detail="No transactions found.")
    content = export_to_pdf_bytes(data, title="Largest Transactions Report")
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=largest_transactions_{account_id}.pdf"},
    )


# --- top accounts ---

@app.get("/accounts/top/by-balance")
def top_accounts_by_balance(
    limit: int = Query(default=5, ge=1, le=100),
):
    conn = get_conn()
    result = get_top_accounts_by_balance(conn, limit=limit)
    
    return result