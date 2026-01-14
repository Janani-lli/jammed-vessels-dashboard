

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import os
import glob
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine # 1.4.50
import warnings

warnings.filterwarnings("ignore")

# Page Config

st.set_page_config(
    page_title="GPS Jamming Monitor",
    layout="wide"
)

st.title(" GPS Jamming Monitoring Dashboard")

# Redshift Connection Config

def start_RS_engine_for_pd():
    url = URL.create(
        drivername="redshift+redshift_connector",
        host=st.secrets["redshift"]["host"],
        port=st.secrets["redshift"]["port"],
        database=st.secrets["redshift"]["database"],
        username=st.secrets["redshift"]["username"],
        password=st.secrets["redshift"]["password"],
    )

    engine = create_engine(url)
    return engine


# Create engine
engine = start_RS_engine_for_pd()


# Persistent Run Storage

RUN_FOLDER = "runs"
os.makedirs(RUN_FOLDER, exist_ok=True)


# SESSION STATE INIT

if "df_current" not in st.session_state:
    st.session_state.df_current = None

if "run_loaded" not in st.session_state:
    st.session_state.run_loaded = False

if "current_run_name" not in st.session_state:
    st.session_state.current_run_name = None


# Sidebar Controls & Alerts

st.sidebar.header("Controls")
fetch_data = st.sidebar.button("Fetch Latest Redshift Data")
compare_prev = st.sidebar.toggle("Compare with previous run", value=True)
last_n_runs = st.sidebar.slider("Number of runs to display in trend", min_value=2, max_value=10, value=5)

if st.session_state.current_run_name:
    st.sidebar.caption(f"Active run: {st.session_state.current_run_name}")


# Helper: Fetch Redshift Snapshot

def fetch_redshift_snapshot():
    query = """
    SELECT *
    FROM lli_prc_dev_gsdbincr_staging.tsw_jammed_events_hist
    """
    return pd.read_sql(query, engine)


# Helper: Save Run Snapshot

def save_run(df):
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(RUN_FOLDER, f"run_{timestamp_str}.csv")
    df.to_csv(filename, index=False)
    return filename


# Fetch & Save Current Run

if fetch_data:
    df_current = fetch_redshift_snapshot()
    # Map Redshift columns
    df_current = df_current.rename(columns={
        "event_start": "timestamp",
        "start_latitude": "latitude",
        "start_longitude": "longitude",
        "opened_eez": "eez_overall"
    })
    df_current["timestamp"] = pd.to_datetime(df_current["timestamp"])

    filename = save_run(df_current)
    st.session_state.df_current = df_current
    st.session_state.current_run_name = os.path.basename(filename)
    st.session_state.run_loaded = True

if not st.session_state.run_loaded:
    st.info("Click **Fetch Latest Redshift Data** to create a new run.")
    st.stop()


# Load Runs & Previous Run Logic

run_files = sorted(glob.glob(os.path.join(RUN_FOLDER, "run_*.csv")))
current_run = (
    st.session_state.df_current
    .sort_values("timestamp")
    .groupby("vesselid", as_index=False)
    .tail(1)
)

if len(run_files) > 1 and compare_prev:
    previous_df = pd.read_csv(run_files[-2])
    previous_run = (
        previous_df
        .sort_values("timestamp")
        .groupby("vesselid", as_index=False)
        .tail(1)
    )
    first_run = False
else:
    previous_run = pd.DataFrame(columns=current_run.columns)
    first_run = True


# Core Logic – New / Resolved Events

current_vessels = set(current_run["vesselid"])
previous_vessels = set(previous_run["vesselid"])
new_vessels = current_vessels - previous_vessels

current_regions = set(current_run["eez_overall"])
previous_regions = set(previous_run["eez_overall"])
new_regions = current_regions - previous_regions
resolved_regions = previous_regions - current_regions


# Sidebar: Smart Alerts

st.sidebar.subheader("Smart Alerts")
if not first_run:
    if new_regions:
        st.sidebar.warning(f"New region detected: {', '.join(new_regions)}")
    if resolved_regions:
        st.sidebar.success(f"Region resolved: {', '.join(resolved_regions)}")
    if previous_vessels and len(current_vessels) > len(previous_vessels):
        increase_pct = ((len(current_vessels) - len(previous_vessels)) / len(previous_vessels)) * 100
        if increase_pct > 10:
            st.sidebar.error(f"> {int(increase_pct)}% increase in jammed vessels!")


# KPIs + Buttons

st.subheader("Key Metrics & New Events")
c1, c2, c3 = st.columns(3)
c1.metric("Total Jammed Vessels (Current Run)", len(current_vessels))
c2.metric("New Jammed Vessels", len(new_vessels))
c3.metric("Active Regions", len(current_regions))

# Button to show new jammed vessels
if st.button("Show New Jammed Vessels"):
    if first_run:
        st.info("This is the first run. All vessels are baseline.")
    else:
        new_df = current_run[current_run["vesselid"].isin(new_vessels)]
        if new_df.empty:
            st.success("No new vessels detected.")
        else:
            st.dataframe(new_df[["vesselid", "eez_overall", "timestamp"]], use_container_width=True)

# Button to show active regions
if st.button("Show Active Regions"):
    region_counts = current_run.groupby("eez_overall")["vesselid"].nunique().reset_index(name="jammed_vessels")
    region_counts["status"] = region_counts["eez_overall"].apply(lambda x: "🆕 New" if x in new_regions else "Active")
    st.dataframe(region_counts, use_container_width=True)


# Interactive Vessel/Region Filter

st.subheader("Event Lookup")
vessel_input = st.text_input("Vessel ID")
region_input = st.text_input("EEZ / Region")
date_range = st.date_input("Date Range", value=[datetime.now().date(), datetime.now().date()])

filtered = current_run.copy()

# Vessel filter
if vessel_input:
    if vessel_input.isdigit():
        filtered = filtered[filtered["vesselid"] == int(vessel_input)]
    else:
        st.warning("Vessel ID must be numeric")

# Region filter
if region_input:
    filtered = filtered[filtered["eez_overall"].fillna("").str.contains(region_input, case=False)]

# Date filter
if date_range and len(date_range) == 2:
    start = pd.to_datetime(date_range[0])
    end = pd.to_datetime(date_range[1])
    filtered = filtered[(filtered["timestamp"] >= start) & (filtered["timestamp"] <= end)]

st.write(f"Events Found: {len(filtered)}")
st.dataframe(filtered[["vesselid", "eez_overall", "timestamp"]], use_container_width=True)


# Jammed Vessels per Region with Previous Run

st.subheader("Jammed Vessels per Region")
current_counts = current_run.groupby("eez_overall")["vesselid"].nunique().reset_index(name="current_count")
previous_counts = previous_run.groupby("eez_overall")["vesselid"].nunique().reset_index(name="prev_count")
merged_counts = pd.merge(current_counts, previous_counts, on="eez_overall", how="outer").fillna(0)

bar_fig = px.bar(
    merged_counts.melt(id_vars="eez_overall", value_vars=["current_count", "prev_count"]),
    x="eez_overall",
    y="value",
    color="variable",
    text="value",
    barmode="group",
    color_discrete_map={"current_count":"blue","prev_count":"green"}
)
st.plotly_chart(bar_fig, use_container_width=True)



# Model Consistency Metrics

st.subheader("Model Consistency Metrics")
c1, c2 = st.columns(2)

# Detection Stability
if not first_run:
    persist_regions = len(current_regions & previous_regions)
    stability_pct = persist_regions / len(previous_regions) * 100 if previous_regions else 100
    c1.metric("Detection Stability (%)", f"{stability_pct:.1f}")

# Event Persistence
all_runs_df = pd.concat([pd.read_csv(f).drop_duplicates("vesselid") for f in run_files[-last_n_runs:]])
persistence = all_runs_df.groupby("vesselid").size().reset_index(name="appearances")
c2.bar_chart(persistence.set_index("vesselid")["appearances"])


# Global Map

st.subheader("Global Jamming Overview")
map_fig = px.scatter_mapbox(
    current_run,
    lat="latitude",
    lon="longitude",
    color="eez_overall",
    hover_name="vesselid",
    zoom=1,
    height=500
)
map_fig.update_layout(mapbox_style="open-street-map")
st.plotly_chart(map_fig, use_container_width=True)



# Multi-Run Trend

st.subheader(f"Trend – Last {last_n_runs} Runs")
trend_runs = run_files[-min(last_n_runs, len(run_files)):]
trend_data = []
for f in trend_runs:
    run_df = pd.read_csv(f).drop_duplicates("vesselid")

    run_time = datetime.strptime(
        os.path.basename(f).replace("run_", "").replace(".csv", ""),
        "%Y%m%d_%H%M%S"
    )

    trend_data.append({
        "run_time": run_time,
        "jammed_vessels": len(run_df)
    })


trend_df = pd.DataFrame(trend_data).sort_values("run_time")
trend_fig = px.line(trend_df, x="run_time", y="jammed_vessels", markers=True, title="Jammed Vessels Trend Across Runs")
st.plotly_chart(trend_fig, use_container_width=True)


st.info("Each fetch creates a new run.")
