import os
import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from plaid.api import plaid_api
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.model.transactions_sync_request_options import TransactionsSyncRequestOptions
from openai import OpenAI

load_dotenv()

# ------------------- Setup Clients -------------------

# Plaid client
from plaid import Configuration, ApiClient
PLAID_CONFIG = Configuration(
    host=Configuration.Host.Sandbox if os.getenv("PLAID_ENV") == "sandbox" else Configuration.Host.Production,
    api_key={
        "clientId": os.getenv("PLAID_CLIENT_ID"),
        "secret": os.getenv("PLAID_SECRET")
    }
)
plaid_client = plaid_api.PlaidApi(ApiClient(PLAID_CONFIG))

# OpenRouter client (OpenAI-compatible)
openai = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENAI_API_KEY"),
    default_headers={"HTTP-Referer": "http://localhost"}  # adjust as needed
)

# PostgreSQL connection
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cursor = conn.cursor(cursor_factory=RealDictCursor)

# ------------------- DB Schema Init -------------------

cursor.execute("""
CREATE TABLE IF NOT EXISTS transactions_raw (
    id SERIAL PRIMARY KEY,
    retrieved_date DATE NOT NULL,
    data JSONB NOT NULL
);
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS recommendations (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    summary TEXT NOT NULL,
    recommendations JSONB NOT NULL
);
""")

# ------------------- Functions -------------------

def fetch_transactions():
    """Fetches yesterday's transactions using Plaid's /transactions/sync."""
    # In a proper production app, you'd persist cursor for incremental sync
    req = TransactionsSyncRequest(
        client_id=os.getenv("PLAID_CLIENT_ID"),
        secret=os.getenv("PLAID_SECRET"),
        cursor=None,
        options=TransactionsSyncRequestOptions()
    )
    resp = plaid_client.transactions_sync(req)
    return resp.to_dict()

def save_raw(data):
    cursor.execute(
        "INSERT INTO transactions_raw (retrieved_date, data) VALUES (%s, %s)",
        (datetime.date.today(), data)
    )

def get_latest_transactions():
    cursor.execute("SELECT data FROM transactions_raw ORDER BY retrieved_date DESC LIMIT 1")
    row = cursor.fetchone()
    return row['data'] if row else None

def generate_insights(transactions_data):
    today = datetime.date.today()
    prompt = (
        f"You are a fintech analyst. Based on the following transaction data for {today}, "
        "provide a short summary and 3 actionable recommendations to improve performance. "
        f"Data:\n{transactions_data}"
    )
    resp = openai.chat.completions.create(
        model="openai/gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful financial analyst."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=300
    )
    content = resp.choices[0].message.content.strip()
    # Optionally parse structured output if needed
    return content

def save_recommendation(summary):
    today = datetime.date.today()
    cursor.execute(
        "INSERT INTO recommendations (date, summary, recommendations) VALUES (%s, %s, %s) "
        "ON CONFLICT (date) DO UPDATE SET summary = EXCLUDED.summary, recommendations = EXCLUDED.recommendations",
        (today, summary, {"text": summary})
    )

# ------------------- Main Flow -------------------

def main():
    raw = fetch_transactions()
    save_raw(raw)
    latest = get_latest_transactions()
    if latest:
        summary = generate_insights(latest)
        save_recommendation(summary)
        print("LLM summary stored for date:", datetime.date.today())
    else:
        print("No transaction data found.")

if __name__ == "__main__":
    main()
