import psycopg2
from src.database import get_connection

DDL = """
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS customers (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    full_name   VARCHAR(255) NOT NULL,
    cpf         CHAR(11) NOT NULL UNIQUE,
    email       VARCHAR(255) NOT NULL UNIQUE,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS accounts (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id     UUID NOT NULL REFERENCES customers(id) ON DELETE RESTRICT,
    account_type    VARCHAR(20) NOT NULL CHECK (account_type IN ('checking', 'savings')),
    balance         NUMERIC(15, 2) NOT NULL DEFAULT 0.00 CHECK (balance >= 0),
    status          VARCHAR(10) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive')),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id      UUID NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    type            VARCHAR(20) NOT NULL CHECK (type IN ('deposit', 'withdrawal', 'transfer_in', 'transfer_out')),
    amount          NUMERIC(15, 2) NOT NULL CHECK (amount > 0),
    description     VARCHAR(255),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

def create_schema():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(DDL)
        conn.commit()
        print("Schema criado com sucesso.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Erro ao criar schema: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_schema()