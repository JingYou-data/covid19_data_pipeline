import streamlit as st
import snowflake.connector
import pandas as pd
import os
import altair as alt
import plotly.express as px
from dotenv import load_dotenv

load_dotenv("/workspace/.env")

st.set_page_config(page_title="COVID-19 Public Health Analytics", layout="wide")


@st.cache_resource
def get_connection():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    conn.cursor().execute(f"USE WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')}")
    return conn


@st.cache_data
def load_data(query):
    conn = get_connection()
    return pd.read_sql(query, conn)


st.title("COVID-19 Public Health Analytics")
st.markdown("National Public Health Analytics Consortium")

st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Go to",
    ["Overview", "Geographic Analysis", "Demographic Vulnerability", "Data Quality"],
)

STATE_ABBREV = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "Puerto Rico": "PR",
}

# ═══════════════════════════════════════════════════════════
# OVERVIEW
# ═══════════════════════════════════════════════════════════
if page == "Overview":
    st.header("National Overview")

    overview_df = load_data(
        """
        SELECT
            STATE,
            YEAR(WEEK_ENDING_DATE)          as year,
            SUM(COVID_19_DEATHS)            as total_covid_deaths,
            AVG(COVID_PCT_OF_TOTAL_DEATHS)  as avg_fatality_rate,
            AVG(PERCENT_OF_EXPECTED_DEATHS) as avg_excess_mortality
        FROM COVID_PIPELINE_DB.GOLD.MART_PUBLIC_HEALTH_KPIS
        WHERE STATE != 'United States'
        AND COVID_19_DEATHS IS NOT NULL
        GROUP BY STATE, YEAR(WEEK_ENDING_DATE)
        ORDER BY year, total_covid_deaths DESC
    """
    )

    years = sorted(overview_df["YEAR"].dropna().unique().astype(int).tolist())
    selected_year = st.select_slider(
        "Select Year", options=years, value=2021, key="overview_year"
    )

    df = (
        overview_df[overview_df["YEAR"] == selected_year]
        .sort_values("TOTAL_COVID_DEATHS", ascending=False)
        .head(10)
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Top 10 States Shown", len(df))
    with col2:
        st.metric("Total COVID Deaths", f"{df['TOTAL_COVID_DEATHS'].sum():,.0f}")
    with col3:
        st.metric("Avg Fatality Rate", f"{df['AVG_FATALITY_RATE'].mean():.1f}%")

    st.subheader(f"Top 10 States by COVID Deaths — {selected_year}")
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("STATE", sort="-y"),
            y="TOTAL_COVID_DEATHS",
            tooltip=["STATE", "TOTAL_COVID_DEATHS", "AVG_FATALITY_RATE"],
        )
    )
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(df)

    # ── US Choropleth Map ────────────────────────────────────
    st.subheader(f"COVID Deaths by State — Map {selected_year}")

    filtered_map = overview_df[overview_df["YEAR"] == selected_year].copy()
    filtered_map["STATE_ABBREV"] = filtered_map["STATE"].map(STATE_ABBREV)
    filtered_map = filtered_map.dropna(subset=["STATE_ABBREV"])

    fig = px.choropleth(
        filtered_map,
        locations="STATE_ABBREV",
        locationmode="USA-states",
        color="TOTAL_COVID_DEATHS",
        scope="usa",
        color_continuous_scale="Reds",
        title=f"COVID-19 Deaths by State — {selected_year}",
        labels={"TOTAL_COVID_DEATHS": "Total Deaths"},
        hover_name="STATE",
    )
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════════════════════════════════════════
# GEOGRAPHIC ANALYSIS
# ═══════════════════════════════════════════════════════════
elif page == "Geographic Analysis":
    st.header("Geographic Analysis")

    geo_df = load_data(
        """
        SELECT
            STATE,
            COUNTY,
            YEAR(DATE_UPDATED)              as year,
            AVG(COVID_CASES_PER_100K)       as avg_cases_per_100k,
            MAX(COVID_19_COMMUNITY_LEVEL)   as community_level,
            AVG(ELDERLY_PCT)                as elderly_pct,
            AVG(POVERTY_PCT)                as poverty_pct,
            MAX(COMMUNITY_LEVEL_SCORE)      as community_level_score
        FROM COVID_PIPELINE_DB.GOLD.MART_GEOGRAPHIC_ANALYSIS
        WHERE COVID_CASES_PER_100K IS NOT NULL
        GROUP BY STATE, COUNTY, YEAR(DATE_UPDATED)
        ORDER BY STATE, COUNTY
    """
    )

    col1, col2 = st.columns(2)
    with col1:
        state_filter = st.selectbox("Select State", sorted(geo_df["STATE"].unique()))
    with col2:
        years = sorted(geo_df["YEAR"].dropna().unique().astype(int).tolist())
        selected_year = st.select_slider(
            "Select Year", options=years, value=2022, key="geo_year"
        )

    filtered = geo_df[
        (geo_df["STATE"] == state_filter) & (geo_df["YEAR"] == selected_year)
    ].sort_values("AVG_CASES_PER_100K", ascending=False)

    st.subheader(f"Counties in {state_filter} — {selected_year} (Cases per 100k)")
    chart = (
        alt.Chart(filtered.head(20))
        .mark_bar()
        .encode(
            x=alt.X("COUNTY", sort="-y"),
            y="AVG_CASES_PER_100K",
            tooltip=[
                "COUNTY",
                "AVG_CASES_PER_100K",
                "COMMUNITY_LEVEL",
                "ELDERLY_PCT",
                "POVERTY_PCT",
            ],
        )
    )
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(filtered)

# ═══════════════════════════════════════════════════════════
# DEMOGRAPHIC VULNERABILITY
# ═══════════════════════════════════════════════════════════
elif page == "Demographic Vulnerability":
    st.header("Demographic Vulnerability")

    dem_df = load_data(
        """
        SELECT
            STATE,
            YEAR(WEEK_ENDING_DATE)              as year,
            AVG(ELDERLY_PCT)                    as avg_elderly_pct,
            AVG(POVERTY_PCT)                    as avg_poverty_pct,
            AVG(ELDERLY_VULNERABILITY_INDEX)    as avg_vulnerability,
            AVG(SOCIOECONOMIC_IMPACT_SCORE)     as avg_socioeconomic_impact,
            MAX(VULNERABILITY_TIER)             as vulnerability_tier
        FROM COVID_PIPELINE_DB.GOLD.MART_DEMOGRAPHIC_VULNERABILITY
        WHERE STATE IS NOT NULL
        GROUP BY STATE, YEAR(WEEK_ENDING_DATE)
        ORDER BY AVG_VULNERABILITY DESC
    """
    )

    years = sorted(dem_df["YEAR"].dropna().unique().astype(int).tolist())
    selected_year = st.select_slider(
        "Select Year", options=years, value=2021, key="dem_year"
    )

    df = (
        dem_df[dem_df["YEAR"] == selected_year]
        .sort_values("AVG_VULNERABILITY", ascending=False)
        .head(15)
    )

    st.subheader(f"Elderly Vulnerability Index by State — {selected_year}")
    chart1 = (
        alt.Chart(df)
        .mark_bar(color="#1A4A72")
        .encode(
            x=alt.X("STATE", sort="-y"),
            y="AVG_VULNERABILITY",
            tooltip=["STATE", "AVG_VULNERABILITY", "VULNERABILITY_TIER"],
        )
    )
    st.altair_chart(chart1, use_container_width=True)

    st.subheader(f"Socioeconomic Impact Score by State — {selected_year}")
    chart2 = (
        alt.Chart(df.sort_values("AVG_SOCIOECONOMIC_IMPACT", ascending=False))
        .mark_bar(color="#B8860B")
        .encode(
            x=alt.X("STATE", sort="-y"),
            y="AVG_SOCIOECONOMIC_IMPACT",
            tooltip=["STATE", "AVG_SOCIOECONOMIC_IMPACT"],
        )
    )
    st.altair_chart(chart2, use_container_width=True)
    st.dataframe(df)

    # ── Scatter: Poverty vs Case Rate ───────────────────────
    st.subheader(f"Poverty Rate vs COVID Cases per 100k — {selected_year}")
    st.markdown("**Does poverty predict higher COVID case rates?**")

    scatter_df = load_data(
        f"""
        SELECT
            d.STATE,
            AVG(d.POVERTY_PCT)              as avg_poverty_pct,
            AVG(g.COVID_CASES_PER_100K)     as avg_cases_per_100k
        FROM COVID_PIPELINE_DB.GOLD.MART_DEMOGRAPHIC_VULNERABILITY d
        LEFT JOIN COVID_PIPELINE_DB.GOLD.MART_GEOGRAPHIC_ANALYSIS g
            ON d.STATE = g.STATE
            AND YEAR(g.DATE_UPDATED) = {selected_year}
        WHERE d.STATE IS NOT NULL
        AND YEAR(d.WEEK_ENDING_DATE) = {selected_year}
        AND g.COVID_CASES_PER_100K IS NOT NULL
        GROUP BY d.STATE
    """
    )

    # 过滤Puerto Rico，只标注top outliers
    scatter_df_clean = scatter_df[scatter_df["STATE"] != "Puerto Rico"].copy()
    # 转成数字类型
    scatter_df_clean["AVG_CASES_PER_100K"] = pd.to_numeric(
        scatter_df_clean["AVG_CASES_PER_100K"], errors="coerce"
    )
    scatter_df_clean["AVG_POVERTY_PCT"] = pd.to_numeric(
        scatter_df_clean["AVG_POVERTY_PCT"], errors="coerce"
    )
    scatter_df_clean = scatter_df_clean.dropna(
        subset=["AVG_CASES_PER_100K", "AVG_POVERTY_PCT"]
    )

    # 找outliers：cases最高的5个 + poverty最高的5个
    top_cases = scatter_df_clean.nlargest(5, "AVG_CASES_PER_100K")["STATE"].tolist()
    top_poverty = scatter_df_clean.nlargest(5, "AVG_POVERTY_PCT")["STATE"].tolist()
    outliers = list(set(top_cases + top_poverty))
    scatter_df_clean["label"] = scatter_df_clean["STATE"].apply(
        lambda x: x if x in outliers else ""
    )

    scatter = (
        alt.Chart(scatter_df_clean)
        .mark_point(size=80, filled=True, color="#5B9EC9")
        .encode(
            x=alt.X("AVG_POVERTY_PCT", title="Poverty Rate (%)"),
            y=alt.Y("AVG_CASES_PER_100K", title="Avg Cases per 100k"),
            tooltip=["STATE", "AVG_POVERTY_PCT", "AVG_CASES_PER_100K"],
        )
    )

    text = (
        alt.Chart(scatter_df_clean)
        .mark_text(dy=-10, fontSize=10)
        .encode(x="AVG_POVERTY_PCT", y="AVG_CASES_PER_100K", text="label")
    )

    st.altair_chart((scatter + text), use_container_width=True)


# ═══════════════════════════════════════════════════════════
# DATA QUALITY
# ═══════════════════════════════════════════════════════════
elif page == "Data Quality":
    st.header("Data Quality")

    st.subheader("Bronze Layer Table Counts")
    col1, col2, col3 = st.columns(3)

    county_count = load_data(
        "SELECT COUNT(*) as cnt FROM COVID_PIPELINE_DB.BRONZE.COUNTY_COVID_DATA"
    )
    deaths_count = load_data(
        "SELECT COUNT(*) as cnt FROM COVID_PIPELINE_DB.BRONZE.COVID_DEATHS_DATA"
    )
    census_count = load_data(
        "SELECT COUNT(*) as cnt FROM COVID_PIPELINE_DB.BRONZE.CENSUS_DATA"
    )

    with col1:
        st.metric("County COVID Records", f"{county_count['CNT'][0]:,.0f}")
    with col2:
        st.metric("Death Count Records", f"{deaths_count['CNT'][0]:,.0f}")
    with col3:
        st.metric("Census Records", f"{census_count['CNT'][0]:,.0f}")

    st.subheader("dbt Test Results")
    col1, col2 = st.columns(2)
    with col1:
        st.success("8 tests passing")
    with col2:
        st.warning(
            "1 warning — CDC privacy suppression (death counts 1-9 not reported)"
        )

    st.subheader("Known Data Quality Issues")
    st.warning(
        "5,335 suppressed death counts due to CDC privacy standards (values 1-9 not reported)"
    )
    st.info("County FIPS codes appear multiple times due to weekly time series data")
