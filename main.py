import os
import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from plaid.api import plaid_api
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.model.transactions_sync_request_options import TransactionsSyncRequestOptions
from plaid import Configuration, ApiClient
from openai import OpenAI

# Load environment variables from .env file (used in local dev)
load_dotenv()

# ------------------- Plaid Configuration -------------------

PLAID_ENV = os.getenv("PLAID_ENV", "sandbox").lower()

if PLAID_ENV == "sandbox":
    PLAID_BASE_URL = "https://sandbox.plaid.com"
elif PLAID_ENV == "development":
    PLAID_BASE_URL = "https://development.plaid.com"
elif PLAID_ENV == "production":
    PLAID_BASE_URL = "https://production.plaid.com"
else:
    raise ValueError("Invalid PLAID_ENV. Use sandbox, development, or production.")

PLAID_CONFIG = Configuration(
    host=PLAID_BASE_URL,
    api_key={
        "clientId": os.getenv("PLAID_CLIENT_ID"),
        "secret": os.getenv("PLAID_SECRET")
    }
)

plaid_client = plaid_api.PlaidApi(ApiClient(PLAID_CONFIG))

# ------------------- OpenRouter (OpenAI-Compatible) -------------------

openai = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENAI_API_KEY"),
    default_headers={"HTTP-Referer": "https://yourdomain.com"}  # Replace with your Railway project domain or GitHub URL
)

# ------------------- PostgreSQL Setup -------------------

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

# ------------------- Pipeline Functions -------------------

def fetch_transactions():
    """Fetches Plaid transactions using /transactions/sync (sandbox sample data)."""
    # Sandbox requires a fixed access token. Normally, you'd get it from link.
    # For sandbox use this static token: 'access-sandbox-...'
    access_token = os.getenv("PLAID_ACCESS_TOKEN")  # Must be set in Railway or .env

    request = TransactionsSyncRequest(
        access_token=access_token,
        cursor=None,  # In production, persist and use the cursor for incremental syncs
        options=TransactionsSyncRequestOptions(
            count=100,
            include_personal_finance_category=True
        )
    )

    response = plaid_client.transactions_sync(request)
    return response.to_dict()

def save_raw(data):
    """Saves full JSON response into the DB."""
    today = datetime.date.today()
    cursor.execute(
        "INSERT INTO transactions_raw (retrieved_date, data) VALUES (%s, %s)",
        (today, data)
    )

def get_latest_transactions():
    """Gets the latest day's raw transaction data."""
    cursor.execute("SELECT data FROM transactions_raw ORDER BY retrieved_date DESC LIMIT 1")
    row = cursor.fetchone()
    return row['data'] if row else None

def generate_insights(transactions_data):
    """Sends data to OpenRouter for fintech insights."""
    today = datetime.date.today()
    prompt = (
        f"You are a fintech analyst. Based on the following transaction data for {today}, "
        "provide a short summary and 3 actionable recommendations to improve financial performance. "
        f"Data:\n{transactions_data}"
    )

    response = openai.chat.completions.create(
        model="openai/gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful financial analyst."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=500
    )

    return response.choices[0].message.content.strip()

def save_recommendation(summary):
    """Stores LLM insights in the database."""
    today = datetime.date.today()
    cursor.execute(
        "INSERT INTO recommendations (date, summary, recommendations) VALUES (%s, %s, %s) "
        "ON CONFLICT (date) DO UPDATE SET summary = EXCLUDED.summary, recommendations = EXCLUDED.recommendations",
        (today, summary, {"text": summary})
    )

# ------------------- Main Script -------------------

def main():
    print("🔄 Starting daily pipeline...")

    try:
        data = fetch_transactions()
        save_raw(data)
        print("✅ Transaction data saved.")

        latest_data = get_latest_transactions()
        if latest_data:
            summary = generate_insights(latest_data)
            save_recommendation(summary)
            print("✅ LLM summary saved to DB.")
        else:
            print("⚠️ No transaction data found for today.")

    except Exception as e:
        print("❌ Error in pipeline:", str(e))

if __name__ == "__main__":
    main()
