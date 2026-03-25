from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

from app.core.config import get_settings
from app.services.repository import list_agencies, list_leads

settings = get_settings()


def load_auth_config() -> dict[str, Any]:
    with open(settings.streamlit_users_yaml_path, "r", encoding="utf-8") as file:
        config = yaml.load(file, Loader=SafeLoader)
    return config


def render_login() -> tuple[bool, str | None]:
    config = load_auth_config()
    authenticator = stauth.Authenticate(
        config["credentials"],
        settings.streamlit_cookie_name,
        settings.streamlit_cookie_key,
        settings.streamlit_cookie_expiry_days,
    )
    authenticator.login(location="main", fields={"Form name": "Login"})
    if st.session_state.get("authentication_status") is True:
        return True, st.session_state.get("name")
    if st.session_state.get("authentication_status") is False:
        st.error("Invalid credentials.")
    else:
        st.info("Enter username and password.")
    return False, None


async def _load_df() -> tuple[pd.DataFrame, pd.DataFrame]:
    leads = await list_leads(limit=500)
    agencies = await list_agencies(limit=500)
    return pd.DataFrame(leads), pd.DataFrame(agencies)


def main() -> None:
    st.set_page_config(page_title="Lead SaaS Dashboard", layout="wide")
    ok, username = render_login()
    if not ok:
        return

    st.title("Lead SaaS Dashboard")
    st.caption(f"Welcome, {username}. JWT-backed auth cookie is active.")
    leads_df, agencies_df = asyncio.run(_load_df())

    c1, c2, c3 = st.columns(3)
    c1.metric("Leads", int(len(leads_df)))
    c2.metric("Agencies", int(len(agencies_df)))
    c3.metric("Updated At", datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))

    st.subheader("Recent Leads")
    if not leads_df.empty:
        st.dataframe(leads_df, use_container_width=True, hide_index=True)
    else:
        st.info("No leads yet.")

    st.subheader("Agencies")
    if not agencies_df.empty:
        st.dataframe(agencies_df, use_container_width=True, hide_index=True)
    else:
        st.info("No agencies yet.")


if __name__ == "__main__":
    main()
