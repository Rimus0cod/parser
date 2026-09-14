import datetime
import json
import logging
from io import BytesIO
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns
import streamlit as st
from PIL import Image, ImageDraw, ImageFont

from app.db.mysql import mysql_pool
from app.services.async_scraper import ScrapedListing

logger = logging.getLogger(__name__)

# Configuration
st.set_page_config(
    page_title="Real Estate Lead Scanner Dashboard",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)


def connect_to_db():
    """Connect to MySQL database."""
    import asyncio

    try:
        pool = asyncio.run(mysql_pool().__aenter__())
        return pool
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None


async def get_listings_data(days: int = 30) -> List[Dict]:
    """Fetch listings data from database."""
    start_date = datetime.date.today() - datetime.timedelta(days=days)

    async with mysql_pool() as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT ad_id, date_seen, title, price, location, size, link, phone,
                           seller_name, ad_type, contact_name, contact_email, source_site
                    FROM listings
                    WHERE date_seen >= %s
                    ORDER BY date_seen DESC, updated_at DESC
                """,
                    (start_date.isoformat(),),
                )

                data = await cur.fetchall()

                columns = [
                    "ad_id",
                    "date_seen",
                    "title",
                    "price",
                    "location",
                    "size",
                    "link",
                    "phone",
                    "seller_name",
                    "ad_type",
                    "contact_name",
                    "contact_email",
                    "source_site",
                ]

                return [dict(zip(columns, row, strict=False)) for row in data]


def extract_price_value(price_str: str) -> Optional[float]:
    """Extract numeric price value from string."""
    if not price_str:
        return None

    import re

    # Extract numeric value (e.g., "700 EUR" -> 700)
    matches = re.findall(r"[\d,.\s]+", price_str)
    if matches:
        num_str = matches[0].replace(",", "").replace(" ", "")
        try:
            return float(num_str)
        except ValueError:
            pass

    return None


def extract_city_from_location(location: str) -> str:
    """Extract city from location string."""
    if not location:
        return "Unknown"

    # Split by comma and take first part (city name)
    parts = location.split(",")
    city = parts[0].strip()

    # Normalize city names
    city_mapping = {
        "Sofia": "Sofia",
        "София": "Sofia",
        "Plovdiv": "Plovdiv",
        "Пловдив": "Plovdiv",
        "Varna": "Varna",
        "Варна": "Varna",
        "Burgas": "Burgas",
        "Бургас": "Burgas",
        "Ruse": "Ruse",
        "Русе": "Ruse",
    }

    return city_mapping.get(city, city)


def create_price_trend_chart(df: pd.DataFrame) -> go.Figure:
    """Create price trend chart."""
    fig = go.Figure()

    df["date_seen"] = pd.to_datetime(df["date_seen"])

    # Calculate average price by date
    daily_avg = df.groupby("date_seen")["price_value"].mean().reset_index()

    fig.add_trace(
        go.Scatter(
            x=daily_avg["date_seen"],
            y=daily_avg["price_value"],
            mode="lines+markers",
            name="Average Price",
            line=dict(color="#FF6B35", width=2),
            marker=dict(size=6),
        )
    )

    fig.update_layout(
        title="Price Trend Over Time",
        xaxis_title="Date",
        yaxis_title="Average Price (€)",
        hovermode="x unified",
        height=400,
    )

    return fig


def create_city_distribution_chart(df: pd.DataFrame) -> go.Figure:
    """Create city distribution chart."""
    city_counts = df["city"].value_counts().head(10)

    fig = px.bar(
        x=city_counts.values,
        y=city_counts.index,
        orientation="h",
        title="Top 10 Cities by Listings Count",
        color_discrete_sequence=["#4ECDC4"],
    )

    fig.update_layout(xaxis_title="Number of Listings", yaxis_title="City", height=400)

    return fig


def create_source_distribution_chart(df: pd.DataFrame) -> go.Figure:
    """Create source site distribution chart."""
    source_counts = df["source_site"].value_counts()

    fig = px.pie(
        values=source_counts.values,
        names=source_counts.index,
        title="Listings by Source Site",
        color_discrete_sequence=px.colors.sequential.Plasma_r,
    )

    fig.update_layout(height=400)

    return fig


def create_ad_type_comparison_chart(df: pd.DataFrame) -> go.Figure:
    """Create ad type comparison chart."""
    type_counts = df["ad_type"].value_counts()

    fig = px.bar(
        x=type_counts.index,
        y=type_counts.values,
        title="Private vs Agency Listings",
        color=type_counts.index,
        color_discrete_map={"private": "#FF6B35", "agency": "#4ECDC4"},
    )

    fig.update_layout(
        xaxis_title="Ad Type", yaxis_title="Number of Listings", showlegend=False, height=400
    )

    return fig


def create_heatmap_price_by_city(df: pd.DataFrame) -> go.Figure:
    """Create heatmap of price by city."""
    # Group by city and calculate average price
    price_by_city = df.groupby("city")["price_value"].mean().round(0).reset_index()

    # Create a pivot table for heatmap
    city_price_pivot = df.pivot_table(
        values="price_value", index="city", columns="ad_type", aggfunc="mean"
    ).fillna(0)

    fig = px.imshow(
        city_price_pivot.values,
        x=city_price_pivot.columns,
        y=city_price_pivot.index,
        color_continuous_scale="Viridis",
        title="Average Price by City and Ad Type",
    )

    fig.update_layout(height=500)

    return fig


def generate_excel_report(df: pd.DataFrame) -> bytes:
    """Generate Excel report with branding."""
    output = BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        # Main data sheet
        df.to_excel(writer, sheet_name="Leads", index=False)

        # Summary sheet
        summary_data = {
            "Metric": ["Total Listings", "Private Ads", "Agency Ads", "Avg Price", "Avg Size"],
            "Value": [
                len(df),
                len(df[df["ad_type"] == "private"]),
                len(df[df["ad_type"] == "agency"]),
                round(df["price_value"].mean(), 2) if not df.empty else 0,
                round(pd.to_numeric(df["size"], errors="coerce").mean(), 2) if not df.empty else 0,
            ],
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)

        workbook = writer.book
        worksheet = writer.sheets["Leads"]
        summary_worksheet = writer.sheets["Summary"]

        # Format cells
        header_format = workbook.add_format(
            {"bold": True, "bg_color": "#4ECDC4", "font_color": "white", "border": 1}
        )

        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

    return output.getvalue()


def generate_pdf_report(df: pd.DataFrame) -> bytes:
    """Generate PDF report with branding."""
    img = Image.new("RGB", (2100, 2970), color=(255, 255, 255))
    draw = ImageDraw.Draw(img)

    try:
        font_large = ImageFont.truetype("arial.ttf", 60)
        font_medium = ImageFont.truetype("arial.ttf", 40)
        font_small = ImageFont.truetype("arial.ttf", 30)
    except:
        font_large = ImageFont.load_default()
        font_medium = ImageFont.load_default()
        font_small = ImageFont.load_default()

    # Header with branding
    draw.text((100, 100), "Real Estate Lead Scanner Report", font=font_large, fill=(0, 0, 0))
    draw.text(
        (100, 180),
        f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        font=font_medium,
        fill=(100, 100, 100),
    )

    # Summary metrics
    y_pos = 300
    metrics = [
        f"Total Listings: {len(df)}",
        f"Private Ads: {len(df[df['ad_type'] == 'private'])}",
        f"Agency Ads: {len(df[df['ad_type'] == 'agency'])}",
        f"Avg Price: €{df['price_value'].mean():.2f}" if not df.empty else "Avg Price: €0.00",
        f"Avg Size: {pd.to_numeric(df['size'], errors='coerce').mean():.2f} sqm"
        if not df.empty
        else "Avg Size: 0.00 sqm",
    ]

    for metric in metrics:
        draw.text((100, y_pos), metric, font=font_medium, fill=(0, 0, 0))
        y_pos += 60

    # Table headers
    headers = ["ID", "Title", "Location", "Price", "Source", "Type", "Date"]
    col_widths = [100, 400, 300, 150, 200, 150, 150]

    x_pos = 100
    for i, (header, width) in enumerate(zip(headers, col_widths)):
        draw.text((x_pos, 700), header, font=font_small, fill=(0, 100, 0))
        x_pos += width

    # Table rows (first 50)
    y_pos = 750
    for i, (_, row) in enumerate(df.head(50).iterrows()):
        if y_pos > 2500:  # Prevent going beyond page
            break

        x_pos = 100
        data_items = [
            str(row.get("ad_id", ""))[:10],
            str(row.get("title", ""))[:30],
            str(row.get("location", ""))[:20],
            str(row.get("price", "")),
            str(row.get("source_site", "")),
            str(row.get("ad_type", "")),
            str(row.get("date_seen", "")),
        ]

        for j, (item, width) in enumerate(zip(data_items, col_widths)):
            draw.text((x_pos, y_pos), item, font=font_small, fill=(0, 0, 0))
            x_pos += width

        y_pos += 50

    # Save to bytes
    img_byte_arr = BytesIO()
    img.save(img_byte_arr, format="PDF")
    img_byte_arr.seek(0)

    return img_byte_arr.read()


async def main():
    st.title("🏠 Real Estate Lead Scanner Dashboard")

    # Sidebar controls
    st.sidebar.header("Filters")

    days = st.sidebar.slider("Days to Show", min_value=1, max_value=365, value=30)

    st.sidebar.markdown("---")
    st.sidebar.header("Export Options")

    export_format = st.sidebar.selectbox("Export Format", ["None", "Excel", "PDF"])

    # Load data
    with st.spinner("Loading listings data..."):
        try:
            listings_data = await get_listings_data(days)
            if not listings_data:
                st.warning("No data available for the selected period.")
                return

            df = pd.DataFrame(listings_data)

            # Process data
            df["price_value"] = df["price"].apply(extract_price_value)
            df["city"] = df["location"].apply(extract_city_from_location)
            df["date_seen"] = pd.to_datetime(df["date_seen"])

        except Exception as e:
            st.error(f"Error loading data: {e}")
            return

    # Main dashboard
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Listings", len(df))
    with col2:
        private_count = len(df[df["ad_type"] == "private"])
        st.metric("Private Ads", private_count)
    with col3:
        agency_count = len(df[df["ad_type"] == "agency"])
        st.metric("Agency Ads", agency_count)
    with col4:
        avg_price = df["price_value"].mean() if not df.empty else 0
        st.metric("Avg Price (€)", f"{avg_price:.0f}")

    # Charts
    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["Price Trends", "City Distribution", "Source Sites", "Ad Types", "Price Heatmap"]
    )

    with tab1:
        st.plotly_chart(create_price_trend_chart(df), use_container_width=True)

    with tab2:
        st.plotly_chart(create_city_distribution_chart(df), use_container_width=True)

    with tab3:
        st.plotly_chart(create_source_distribution_chart(df), use_container_width=True)

    with tab4:
        st.plotly_chart(create_ad_type_comparison_chart(df), use_container_width=True)

    with tab5:
        st.plotly_chart(create_heatmap_price_by_city(df), use_container_width=True)

    # Data table
    st.header("Latest Listings")

    # Filters for the table
    col1, col2 = st.columns(2)

    with col1:
        ad_type_filter = st.selectbox("Ad Type", ["All", "private", "agency"], index=0)

    with col2:
        source_filter = st.selectbox("Source Site", ["All"] + list(df["source_site"].unique()))

    # Apply filters
    filtered_df = df.copy()
    if ad_type_filter != "All":
        filtered_df = filtered_df[filtered_df["ad_type"] == ad_type_filter]

    if source_filter != "All":
        filtered_df = filtered_df[filtered_df["source_site"] == source_filter]

    # Display table
    display_cols = [
        "ad_id",
        "date_seen",
        "title",
        "price",
        "location",
        "size",
        "source_site",
        "ad_type",
        "link",
    ]
    st.dataframe(filtered_df[display_cols].head(50), use_container_width=True, hide_index=True)

    # Export functionality
    if export_format != "None":
        if export_format == "Excel":
            excel_data = generate_excel_report(filtered_df)
            st.download_button(
                label="Download Excel Report",
                data=excel_data,
                file_name=f"real_estate_leads_{datetime.date.today().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.ms-excel",
            )
        elif export_format == "PDF":
            pdf_data = generate_pdf_report(filtered_df)
            st.download_button(
                label="Download PDF Report",
                data=pdf_data,
                file_name=f"real_estate_leads_{datetime.date.today().strftime('%Y%m%d')}.pdf",
                mime="application/pdf",
            )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
