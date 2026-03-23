"""
Streamlit Admin Dashboard for Imoti.bg Lead Scraper.
Run:  streamlit run dashboard.py
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from dotenv import load_dotenv

# ── Load .env ────────────────────────────────────────────────────────────────
_ENV_FILE = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_ENV_FILE, override=False)

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Imoti Lead Scanner",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS injection for premium look ──────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* Metric cards */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        border: 1px solid #475569;
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
    }
    div[data-testid="stMetric"] label {
        color: #94a3b8 !important;
        font-size: 0.8rem !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #f1f5f9 !important;
        font-weight: 700 !important;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
        border-right: 1px solid #334155;
    }
    section[data-testid="stSidebar"] .stRadio label {
        color: #cbd5e1 !important;
    }

    /* Data table styling */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
    }

    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        padding: 0.5rem 1.5rem !important;
        transition: all 0.2s ease !important;
    }
    .stButton > button:hover {
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.4) !important;
        transform: translateY(-1px) !important;
    }

    /* Success button override for Run Scraper */
    div[data-testid="stHorizontalBlock"] .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #10b981, #059669) !important;
    }

    /* Header gradient */
    .main-header {
        background: linear-gradient(135deg, #1e3a5f 0%, #0f172a 50%, #1a1a2e 100%);
        padding: 24px 32px;
        border-radius: 16px;
        margin-bottom: 24px;
        border: 1px solid #334155;
    }
    .main-header h1 {
        margin: 0;
        font-size: 1.8rem;
        font-weight: 700;
        background: linear-gradient(90deg, #60a5fa, #a78bfa);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .main-header p {
        margin: 4px 0 0 0;
        color: #94a3b8;
        font-size: 0.9rem;
    }

    /* Status badge */
    .status-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.03em;
    }
    .status-private {
        background: rgba(16, 185, 129, 0.15);
        color: #34d399;
        border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .status-agency {
        background: rgba(251, 146, 60, 0.15);
        color: #fb923c;
        border: 1px solid rgba(251, 146, 60, 0.3);
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ──────────────────────────────────────────────────────────────────────────────
# Database helpers
# ──────────────────────────────────────────────────────────────────────────────

def _get_connection():
    """Return a MySQL connection using .env credentials."""
    import mysql.connector  # type: ignore[import-not-found]

    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "imoti"),
        charset="utf8mb4",
        use_unicode=True,
    )


@st.cache_data(ttl=120)
def _load_listings() -> pd.DataFrame:
    conn = _get_connection()
    df = pd.read_sql(
        "SELECT * FROM listings ORDER BY date_seen DESC, updated_at DESC",
        conn,
    )
    conn.close()
    return df


@st.cache_data(ttl=120)
def _load_agencies() -> pd.DataFrame:
    conn = _get_connection()
    df = pd.read_sql("SELECT * FROM agencies ORDER BY agency_name", conn)
    conn.close()
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Auth  (simple password gate — enough for Stage 1)
# ──────────────────────────────────────────────────────────────────────────────

_DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "admin")


def _check_auth() -> bool:
    if st.session_state.get("authenticated"):
        return True

    st.markdown(
        """
        <div style="display:flex;align-items:center;justify-content:center;
                     min-height:60vh;flex-direction:column;">
            <div style="text-align:center;margin-bottom:40px;">
                <span style="font-size:3.5rem;">🏠</span>
                <h1 style="margin:8px 0 0;font-weight:700;
                           background:linear-gradient(90deg,#60a5fa,#a78bfa);
                           -webkit-background-clip:text;-webkit-text-fill-color:transparent;">
                    Imoti Lead Scanner</h1>
                <p style="color:#94a3b8;margin:4px 0 0;">Admin Dashboard</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        pwd = st.text_input("Пароль", type="password", key="login_password")
        if st.button("Увійти", use_container_width=True):
            if pwd == _DASHBOARD_PASSWORD:
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("❌ Невірний пароль")
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Sidebar navigation
# ──────────────────────────────────────────────────────────────────────────────

_PAGES = {
    "📊 Дашборд": "dashboard",
    "🏠 Приватні ліди": "private_leads",
    "🏢 Агенції": "agencies",
    "⚙️ Налаштування": "settings",
}


def _sidebar():
    with st.sidebar:
        st.markdown(
            "<div style='padding:12px 0;text-align:center;'>"
            "<span style='font-size:2rem;'>🏠</span><br>"
            "<span style='font-weight:700;font-size:1.1rem;"
            "background:linear-gradient(90deg,#60a5fa,#a78bfa);"
            "-webkit-background-clip:text;-webkit-text-fill-color:transparent;'>"
            "Lead Scanner</span></div>",
            unsafe_allow_html=True,
        )
        st.divider()
        page = st.radio(
            "Навігація",
            list(_PAGES.keys()),
            label_visibility="collapsed",
        )
        st.divider()
        if st.button("🔄 Оновити дані", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        st.divider()
        if st.button("🚀 Запустити парсер", use_container_width=True):
            _run_scraper()
        if st.button("🚪 Вийти", use_container_width=True):
            st.session_state["authenticated"] = False
            st.rerun()
    return _PAGES[page]


# ──────────────────────────────────────────────────────────────────────────────
# Pages
# ──────────────────────────────────────────────────────────────────────────────

def _page_dashboard():
    st.markdown(
        '<div class="main-header">'
        "<h1>📊 Панель моніторингу</h1>"
        "<p>Огляд зібраних даних за останні дні</p>"
        "</div>",
        unsafe_allow_html=True,
    )

    df = _load_listings()
    if df.empty:
        st.info("Поки немає даних. Запустіть парсер для збору оголошень.")
        return

    total = len(df)
    private = len(df[df["ad_type"] == "приватний"])
    agency = len(df[df["ad_type"] == "від агенції"])
    with_phone = len(df[df["phone"].fillna("").str.len() > 0])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Всього оголошень", total)
    c2.metric("🟢 Приватні", private)
    c3.metric("🟠 Від агенцій", agency)
    c4.metric("📞 З телефоном", with_phone)

    st.markdown("---")

    # ── Chart: leads per day ──────────────────────────────────────────────
    if "date_seen" in df.columns:
        daily = (
            df.dropna(subset=["date_seen"])
            .groupby(["date_seen", "ad_type"])
            .size()
            .reset_index(name="count")
        )
        if not daily.empty:
            st.subheader("Динаміка лідів по днях")
            import altair as alt

            chart = (
                alt.Chart(daily)
                .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                .encode(
                    x=alt.X("date_seen:T", title="Дата"),
                    y=alt.Y("count:Q", title="Кількість"),
                    color=alt.Color(
                        "ad_type:N",
                        title="Тип",
                        scale=alt.Scale(
                            domain=["приватний", "від агенції"],
                            range=["#34d399", "#fb923c"],
                        ),
                    ),
                    tooltip=["date_seen:T", "ad_type:N", "count:Q"],
                )
                .properties(height=320)
            )
            st.altair_chart(chart, use_container_width=True)

    # ── Recent leads ─────────────────────────────────────────────────────
    st.subheader("Останні 20 оголошень")
    recent = df.head(20)[
        ["date_seen", "ad_id", "title", "price", "location", "phone", "seller_name", "ad_type"]
    ].copy()
    st.dataframe(recent, use_container_width=True, hide_index=True)


def _page_private_leads():
    st.markdown(
        '<div class="main-header">'
        "<h1>🏠 Приватні ліди</h1>"
        "<p>Оголошення від приватних власників — ваші потенційні клієнти</p>"
        "</div>",
        unsafe_allow_html=True,
    )

    df = _load_listings()
    private_df = df[df["ad_type"] == "приватний"].copy()

    if private_df.empty:
        st.info("Приватних оголошень поки не знайдено.")
        return

    # ── Filters ──────────────────────────────────────────────────────────
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        cities = sorted(private_df["location"].dropna().unique().tolist())
        city = st.selectbox("📍 Місто", ["Всі"] + cities)
    with col_f2:
        with_phone_only = st.checkbox("📞 Тільки з телефоном", value=True)
    with col_f3:
        date_range = st.selectbox(
            "📅 Період",
            ["Всі", "Сьогодні", "Останні 3 дні", "Останній тиждень"],
        )

    # Apply filters
    if city != "Всі":
        private_df = private_df[private_df["location"].str.contains(city, case=False, na=False)]
    if with_phone_only:
        private_df = private_df[private_df["phone"].fillna("").str.len() > 0]
    if date_range != "Всі" and "date_seen" in private_df.columns:
        today = date.today()
        if date_range == "Сьогодні":
            private_df = private_df[private_df["date_seen"] == str(today)]
        elif date_range == "Останні 3 дні":
            cutoff = today - timedelta(days=3)
            private_df = private_df[private_df["date_seen"] >= str(cutoff)]
        elif date_range == "Останній тиждень":
            cutoff = today - timedelta(days=7)
            private_df = private_df[private_df["date_seen"] >= str(cutoff)]

    st.metric("Знайдено лідів", len(private_df))

    display = private_df[
        ["date_seen", "ad_id", "title", "price", "location", "size", "phone", "seller_name", "link"]
    ].copy()
    display.columns = [
        "Дата", "ID", "Назва", "Ціна", "Місто", "Площа", "📞 Телефон", "Продавець", "🔗 Посилання"
    ]

    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "🔗 Посилання": st.column_config.LinkColumn("🔗 Посилання", display_text="Переглянути"),
        },
    )

    # ── CSV Export ────────────────────────────────────────────────────────
    csv = display.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "📥 Експорт у CSV",
        csv,
        file_name=f"private_leads_{date.today().isoformat()}.csv",
        mime="text/csv",
    )


def _page_agencies():
    st.markdown(
        '<div class="main-header">'
        "<h1>🏢 База агенцій</h1>"
        "<p>Збережені контактні дані всіх відомих агенцій</p>"
        "</div>",
        unsafe_allow_html=True,
    )

    df = _load_agencies()
    if df.empty:
        st.info("Агенції ще не завантажені. Запустіть `python scraper.py --update-agencies`.")
        return

    st.metric("Всього агенцій", len(df))

    search = st.text_input("🔍 Пошук агенції")
    if search:
        mask = df.apply(lambda r: search.lower() in str(r).lower(), axis=1)
        df = df[mask]

    display = df[["agency_name", "phones", "city", "email", "contact_name"]].copy()
    display.columns = ["Назва", "Телефони", "Місто", "Email", "Контактна особа"]
    st.dataframe(display, use_container_width=True, hide_index=True)


def _page_settings():
    st.markdown(
        '<div class="main-header">'
        "<h1>⚙️ Налаштування</h1>"
        "<p>Конфігурація парсера та сповіщень</p>"
        "</div>",
        unsafe_allow_html=True,
    )

    tab1, tab2, tab3 = st.tabs(["🤖 Telegram", "🌐 Парсер", "ℹ️ Статус"])

    with tab1:
        st.subheader("Telegram-сповіщення")
        token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        status = "🟢 Налаштовано" if token and chat_id else "🔴 Не налаштовано"
        st.markdown(f"**Статус:** {status}")
        st.info(
            "Для налаштування Telegram-сповіщень додайте `TELEGRAM_BOT_TOKEN` "
            "та `TELEGRAM_CHAT_ID` у файл `.env` та перезапустіть додаток."
        )
        if token and chat_id:
            if st.button("✉️ Надіслати тестове повідомлення"):
                _send_test_telegram(token, chat_id)

    with tab2:
        st.subheader("Параметри парсера")
        st.markdown(f"**Макс. сторінок:** `{os.getenv('MAX_PAGES', '30')}`")
        st.markdown(f"**Затримка (сек):** `{os.getenv('REQUEST_DELAY_MIN', '2.0')}` – `{os.getenv('REQUEST_DELAY_MAX', '5.0')}`")
        st.markdown(f"**Фільтр міста:** `{os.getenv('CITY_FILTER', '—') or '—'}`")

    with tab3:
        st.subheader("Статус системи")
        try:
            conn = _get_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM listings")
            listings_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM agencies")
            agencies_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM processed_ids")
            processed_count = cur.fetchone()[0]
            conn.close()
            st.markdown(f"- **MySQL:** 🟢 Підключено")
            st.markdown(f"- **Оголошень у БД:** `{listings_count}`")
            st.markdown(f"- **Агенцій у БД:** `{agencies_count}`")
            st.markdown(f"- **Оброблених ID:** `{processed_count}`")
        except Exception as e:
            st.error(f"❌ Помилка підключення до MySQL: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _run_scraper():
    """Launch the scraper as a subprocess."""
    venv_python = Path(__file__).parent / "venv" / "Scripts" / "python.exe"
    if not venv_python.exists():
        venv_python = Path(__file__).parent / "venv" / "bin" / "python"
    if not venv_python.exists():
        venv_python = sys.executable

    with st.spinner("🔄 Парсер працює…"):
        result = subprocess.run(
            [str(venv_python), "scraper.py"],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent),
            timeout=600,
        )
    if result.returncode == 0:
        st.success("✅ Парсер завершив роботу!")
        st.cache_data.clear()
    else:
        st.error("❌ Парсер завершився з помилкою")
        with st.expander("Деталі"):
            st.code(result.stderr or result.stdout)


def _send_test_telegram(token: str, chat_id: str):
    """Send a test message to verify Telegram setup."""
    import requests

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": "✅ Тестове повідомлення від Imoti Lead Scanner!\n\nВаш Telegram-бот працює коректно.",
        "parse_mode": "HTML",
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.ok:
            st.success("✅ Тестове повідомлення надіслано!")
        else:
            st.error(f"Помилка Telegram API: {resp.text}")
    except Exception as e:
        st.error(f"Не вдалося надіслати: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    if not _check_auth():
        return

    page = _sidebar()

    if page == "dashboard":
        _page_dashboard()
    elif page == "private_leads":
        _page_private_leads()
    elif page == "agencies":
        _page_agencies()
    elif page == "settings":
        _page_settings()


if __name__ == "__main__":
    main()
