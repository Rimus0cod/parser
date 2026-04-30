from __future__ import annotations

# ruff: noqa: E402

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth
import yaml  # type: ignore[import-untyped, unused-ignore]
from yaml.loader import SafeLoader  # type: ignore[import-untyped, unused-ignore]

# Streamlit may execute this file as a standalone script, so keep the repo root
# on sys.path to preserve absolute imports like `app.core.config`.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import get_settings
from app.core.config import validate_runtime_settings
from app.services.repository import (
    list_agencies,
    list_leads,
    list_listing_issues,
    list_review_leads,
    list_tenant_contacts,
    list_voice_calls,
    upsert_tenant_contacts,
)
from app.voice.runtime import get_voice_service
from app.voice.service import parse_tenant_contacts_csv

settings = get_settings()


def load_auth_config() -> dict[str, Any]:
    config_path = Path(settings.streamlit_users_yaml_path)
    if not config_path.exists():
        raise RuntimeError(
            f"Streamlit users config is missing: {config_path}. "
            "Create the file before starting the dashboard."
        )
    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.load(file, Loader=SafeLoader)
    usernames = config.get("credentials", {}).get("usernames", {})
    if not usernames:
        raise RuntimeError("Streamlit users config does not define any usernames.")
    return cast(dict[str, Any], config)


def render_login() -> tuple[bool, str | None]:
    try:
        config = load_auth_config()
    except (OSError, RuntimeError, yaml.YAMLError) as exc:
        st.error(f"Dashboard authentication config error: {exc}")
        return False, None
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


async def _load_dashboard_frames() -> (
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
):
    leads = await list_leads(limit=250)
    review_leads = await list_review_leads(limit=250)
    listing_issues = await list_listing_issues(limit=250)
    agencies = await list_agencies(limit=250)
    voice_calls = await list_voice_calls(limit=250)
    tenant_contacts = await list_tenant_contacts(limit=250)
    return (
        pd.DataFrame(leads),
        pd.DataFrame(review_leads),
        pd.DataFrame(listing_issues),
        pd.DataFrame(agencies),
        pd.DataFrame(voice_calls),
        pd.DataFrame(tenant_contacts),
    )


async def _start_voice_call(
    listing_ad_id: str,
    listing_source_site: str | None,
    initiated_by: str,
) -> dict[str, Any]:
    return await get_voice_service().start_listing_call(
        listing_ad_id=listing_ad_id,
        listing_source_site=listing_source_site,
        initiated_by=initiated_by,
    )


async def _import_tenants(rows: list[dict[str, Any]]) -> int:
    return await upsert_tenant_contacts(rows)


def _format_answers(value: Any) -> str:
    if isinstance(value, dict) and value:
        return "; ".join(f"{key}: {answer}" for key, answer in value.items())
    return "-"


def _render_overview(
    leads_df: pd.DataFrame,
    agencies_df: pd.DataFrame,
    voice_calls_df: pd.DataFrame,
    tenant_contacts_df: pd.DataFrame,
) -> None:
    metrics = st.columns(5)
    metrics[0].metric("Leads", int(len(leads_df)))
    metrics[1].metric("Agencies", int(len(agencies_df)))
    metrics[2].metric("Voice Calls", int(len(voice_calls_df)))
    metrics[3].metric("Tenant Contacts", int(len(tenant_contacts_df)))
    metrics[4].metric("Updated At", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))


def _render_leads_tab(leads_df: pd.DataFrame, username: str | None) -> None:
    st.subheader("Recent Leads")
    if leads_df.empty:
        st.info("No leads yet.")
        return

    display_columns = [
        column
        for column in (
            "ad_id",
            "title",
            "price",
            "location",
            "phone",
            "seller_name",
            "contact_name",
        )
        if column in leads_df.columns
    ]
    if "source_site" in leads_df.columns and "source_site" not in display_columns:
        display_columns.insert(1, "source_site")
    launch_df = leads_df[display_columns].copy()
    launch_df.insert(0, "launch", False)
    edited_df = st.data_editor(
        launch_df,
        use_container_width=True,
        hide_index=True,
        disabled=[column for column in launch_df.columns if column != "launch"],
        column_config={
            "launch": st.column_config.CheckboxColumn(
                "Call", help="Select one lead to start a call."
            ),
        },
        key="voice_launch_editor",
    )

    if st.button("Start Voice Call For Selected Lead", type="primary"):
        selected_rows = edited_df[edited_df["launch"]]
        if selected_rows.empty:
            st.warning("Select one lead first.")
        elif len(selected_rows) > 1:
            st.warning("Select only one lead at a time.")
        else:
            listing_ad_id = str(selected_rows.iloc[0]["ad_id"])
            listing_source_site = (
                str(selected_rows.iloc[0]["source_site"])
                if "source_site" in selected_rows.columns
                else None
            )
            try:
                result = asyncio.run(
                    _start_voice_call(listing_ad_id, listing_source_site, username or "streamlit")
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"Voice call could not be created: {exc}")
            else:
                st.success(f"Voice call #{result['id']} created for listing {listing_ad_id}.")

    st.dataframe(leads_df, use_container_width=True, hide_index=True)


def _render_quality_tab(review_leads_df: pd.DataFrame, listing_issues_df: pd.DataFrame) -> None:
    st.subheader("Manual Review")
    if review_leads_df.empty:
        st.info("No leads currently require manual review.")
    else:
        st.dataframe(review_leads_df, use_container_width=True, hide_index=True)

    st.subheader("Extraction Issues")
    if listing_issues_df.empty:
        st.info("No parser extraction issues recorded for the current parser version.")
    else:
        st.dataframe(listing_issues_df, use_container_width=True, hide_index=True)


def _render_agencies_tab(agencies_df: pd.DataFrame) -> None:
    st.subheader("Agencies")
    if agencies_df.empty:
        st.info("No agencies yet.")
        return
    st.dataframe(agencies_df, use_container_width=True, hide_index=True)


def _render_voice_calls_tab(voice_calls_df: pd.DataFrame) -> None:
    st.subheader("Voice Calls")
    if voice_calls_df.empty:
        st.info("No voice calls yet.")
        return

    display_df = voice_calls_df.copy()
    if "answers_json" in display_df.columns:
        display_df["answers_summary"] = display_df["answers_json"].apply(_format_answers)
    visible_columns = [
        column
        for column in (
            "id",
            "status",
            "listing_ad_id",
            "listing_title",
            "contact_name",
            "phone_e164",
            "answers_summary",
            "recording_url",
            "created_at",
            "updated_at",
        )
        if column in display_df.columns
    ]
    st.dataframe(display_df[visible_columns], use_container_width=True, hide_index=True)

    selected_call_id = st.selectbox(
        "Inspect voice call",
        options=[int(call_id) for call_id in display_df["id"].tolist()],
    )
    selected_rows = display_df[display_df["id"] == selected_call_id]
    if selected_rows.empty:
        return
    selected = selected_rows.iloc[0].to_dict()

    c1, c2 = st.columns(2)
    c1.markdown(f"**Status:** {selected.get('status', '-')}")
    c1.markdown(f"**Listing:** {selected.get('listing_title', '-')}")
    c1.markdown(f"**Phone:** {selected.get('phone_e164', '-')}")
    c2.markdown(f"**Started:** {selected.get('started_at', '-')}")
    c2.markdown(f"**Answered:** {selected.get('answered_at', '-')}")
    c2.markdown(f"**Completed:** {selected.get('completed_at', '-')}")

    st.write("Structured Answers")
    st.code(
        json.dumps(selected.get("answers_json") or {}, ensure_ascii=False, indent=2),
        language="json",
    )
    st.write("Transcript")
    st.text(selected.get("transcript") or "-")
    if selected.get("recording_url"):
        st.link_button("Open Recording", str(selected["recording_url"]))


def _render_tenant_contacts_tab(tenant_contacts_df: pd.DataFrame) -> None:
    st.subheader("Tenant Contacts")
    uploaded_file = st.file_uploader("Import tenant CSV", type=["csv"])
    if uploaded_file is not None and st.button("Import Tenant Contacts"):
        rows = parse_tenant_contacts_csv(uploaded_file.getvalue(), filename=uploaded_file.name)
        if not rows:
            st.warning("No valid Bulgarian phone numbers were found in the uploaded CSV.")
        else:
            imported = asyncio.run(_import_tenants(rows))
            st.success(f"Imported or updated {imported} tenant contacts.")

    if tenant_contacts_df.empty:
        st.info("No tenant contacts yet.")
        return
    st.dataframe(tenant_contacts_df, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Lead SaaS Dashboard", layout="wide")
    try:
        validate_runtime_settings(settings, component="streamlit")
    except RuntimeError as exc:
        st.error(str(exc))
        return
    ok, username = render_login()
    if not ok:
        return

    st.title("Lead SaaS Dashboard")
    st.caption(f"Welcome, {username}. JWT-backed auth cookie is active.")
    if not settings.voice_enabled:
        st.info(
            "Voice integration is disabled. Set `VOICE_ENABLED=true` to enable outbound calling."
        )

    (
        leads_df,
        review_leads_df,
        listing_issues_df,
        agencies_df,
        voice_calls_df,
        tenant_contacts_df,
    ) = asyncio.run(_load_dashboard_frames())
    _render_overview(leads_df, agencies_df, voice_calls_df, tenant_contacts_df)

    leads_tab, quality_tab, agencies_tab, voice_tab, tenants_tab = st.tabs(
        ["Leads", "Parser Quality", "Agencies", "Voice Calls", "Tenant Contacts"]
    )
    with leads_tab:
        _render_leads_tab(leads_df, username)
    with quality_tab:
        _render_quality_tab(review_leads_df, listing_issues_df)
    with agencies_tab:
        _render_agencies_tab(agencies_df)
    with voice_tab:
        _render_voice_calls_tab(voice_calls_df)
    with tenants_tab:
        _render_tenant_contacts_tab(tenant_contacts_df)


if __name__ == "__main__":
    main()
