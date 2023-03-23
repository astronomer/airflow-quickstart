# --------------- #
# PACKAGE IMPORTS #
# --------------- #

import streamlit as st
import duckdb
import pandas as pd
from datetime import date, datetime
import altair as alt
import global_variables.user_input_variables as uv
import global_variables.constants as c

# --------- #
# VARIABLES #
# --------- #

city_name = uv.MY_CITY
user_name = uv.MY_NAME
hot_day = uv.HOT_DAY


duck_db_instance_path = (
    "/app/include/dwh"  # when changing this value also change the db name in .env
)
global_temp_col = "Global"
metric_col_name = "Average Surface Temperature"
date_col_name = "date"
decade_grain_col_name = "decade_average_temp"
year_grain_col_name = "year_average_temp"

# -------------- #
# DuckDB Queries #
# -------------- #


def list_currently_available_tables(db=duck_db_instance_path):
    cursor = duckdb.connect(db)
    tables = cursor.execute("SHOW TABLES;").fetchall()
    cursor.close()
    return [table[0] for table in tables]


def get_current_weather_info_by_city(city, db=duck_db_instance_path):
    cursor = duckdb.connect(db)
    row = cursor.execute(
        f"""SELECT lat, long, temperature, windspeed, winddirection, api_response
        FROM {c.IN_CURRENT_WEATHER_TABLE_NAME}
        WHERE city == '{city}' ORDER BY time DESC LIMIT 1;"""
    ).fetchall()
    cursor.close()
    return row[0]


def get_global_surface_temp_data(db=duck_db_instance_path):

    cursor = duckdb.connect(db)

    # get global surface temperature data
    global_surface_temp_data = cursor.execute(
        f"""SELECT * 
        FROM {c.REPORT_CLIMATE_TABLE_NAME};"""
    ).fetchall()

    global_surface_temp_col_names = cursor.execute(
        f"""SELECT column_name from information_schema.columns where table_name = '{c.REPORT_CLIMATE_TABLE_NAME}';"""
    ).fetchall()

    cursor.close()

    df = pd.DataFrame(
        global_surface_temp_data, columns=[x[0] for x in global_surface_temp_col_names]
    )

    return df


def get_historic_weather_info(db=duck_db_instance_path):

    cursor = duckdb.connect(db)
    historical_weather_data = cursor.execute(
        f"""SELECT * FROM {c.REPORT_HISTORICAL_WEATHER_TABLE_NAME};"""
    ).fetchall()

    historical_weather_col_names = cursor.execute(
        f"""SELECT column_name from information_schema.columns where table_name = '{c.REPORT_HISTORICAL_WEATHER_TABLE_NAME}';"""
    ).fetchall()

    df = pd.DataFrame(
        historical_weather_data, columns=[x[0] for x in historical_weather_col_names]
    )
    cursor.close()

    df["Year"] = pd.to_datetime(df["time"])
    df["Heat days per year"] = df["heat_days_per_year"]

    return df


def get_hot_days(db=duck_db_instance_path):
    cursor = duckdb.connect(db)

    hot_days_col_names = cursor.execute(
        f"""SELECT column_name from information_schema.columns where table_name = '{c.REPORT_HOT_DAYS_TABLE_NAME}';"""
    ).fetchall()

    hot_days_data = cursor.execute(
        f"""SELECT * FROM {c.REPORT_HOT_DAYS_TABLE_NAME};"""
    ).fetchall()
    df = pd.DataFrame(hot_days_data, columns=[x[0] for x in hot_days_col_names])
    return df


# ------------ #
# Query DuckDB #
# ------------ #


tables = list_currently_available_tables()

if c.IN_CURRENT_WEATHER_TABLE_NAME in tables:
    (
        my_city_lat,
        my_city_long,
        my_city_current_temp,
        my_city_current_windspeed,
        my_city_current_winddirection,
        my_city_api_response,
    ) = get_current_weather_info_by_city(city_name)

if c.REPORT_CLIMATE_TABLE_NAME in tables:
    global_temp_df = get_global_surface_temp_data()
    global_temp_df["Scale"] = "Global"

if c.REPORT_HISTORICAL_WEATHER_TABLE_NAME in tables:
    historical_weather_table = get_historic_weather_info()

if c.REPORT_HOT_DAYS_TABLE_NAME in tables:
    hot_days_table = get_hot_days()

# ------------- #
# STREAMLIT APP #
# ------------- #

st.title("Climate and Weather Dashboard")

st.markdown(f"Hello {user_name} :wave: Welcome to your Streamlit App! :blush:")

st.subheader("Global surface temperatures")

# --------------- #
# Climate section #
# --------------- #

if c.REPORT_CLIMATE_TABLE_NAME in tables:
    # define columns
    col1, col2 = st.columns([3, 2])

    # col 1 contains the time-period slider
    with col1:

        # add slider
        timespan = st.slider(
            "Adjust the time-period shown.",
            value=(date(1760, 1, 1), date(2023, 1, 1)),
            format="YYYY",
        )

        global_temp_df_timecut = global_temp_df[
            (global_temp_df[date_col_name] >= timespan[0])
            & (global_temp_df[date_col_name] <= timespan[1])
        ]

    # col 2 contains the check-boxes for global data and user-selected country
    with col2:

        # add selectbox for grain of data to display
        grain = st.selectbox(
            "Average temperatures per",
            ("Decade", "Year", "Raw Data"),
            label_visibility="visible",
            index=0,
        )

    # get interactive chart
    def get_chart(data, grain):
        """Function to return interactive chart."""

        # adjust grain based on selectbox input
        if grain == "Decade":
            y = decade_grain_col_name
        if grain == "Year":
            y = year_grain_col_name
        if grain == "Raw Data":
            y = "day_average_temp"

        # add display of data when hovering
        hover = alt.selection_single(
            fields=[date_col_name],
            nearest=True,
            on="mouseover",
            empty="none",
        )

        # add lines
        lines = alt.Chart(data).mark_line().encode(x=date_col_name, y=y)

        # draw points on the line, and highlight based on selection
        points = lines.transform_filter(hover).mark_circle(size=65)

        # draw a ruler at the location of the selection
        tooltips = (
            alt.Chart(data)
            .mark_rule()
            .encode(
                x=date_col_name,
                y=y,
                opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
                tooltip=[
                    alt.Tooltip(date_col_name, title="Date"),
                    alt.Tooltip(y, title="Average Surface °C", format=".2f"),
                ],
            )
            .add_selection(hover)
        )

        return (lines + points + tooltips).interactive()

    chart = get_chart(global_temp_df_timecut, grain)

    # plot climate chart
    st.altair_chart((chart).interactive(), use_container_width=True)
else:
    st.markdown(
        "Run part 1 of the Airflow pipeline to get an interactive chart with global climate data!"
    )


# ------------------------------ #
# Current weather in main cities #
# ------------------------------ #

st.subheader(f"Current weather in {city_name}")
if c.IN_CURRENT_WEATHER_TABLE_NAME in tables:
    if my_city_api_response == 200:

        # create 3 columns for metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("Temperature [°C]", round(my_city_current_temp, 1))
        col2.metric("Windspeed [km/h]", round(my_city_current_windspeed, 2))
        col3.metric(
            "Winddirection [° clockwise from north]", my_city_current_winddirection
        )

        # plot location of user-defined city
        city_coordinates_df = pd.DataFrame(
            [(my_city_lat, my_city_long)], columns=["lat", "lon"]
        )

        st.map(city_coordinates_df)

    else:
        st.markdown(
            f"""Your call to the open weather API returned
            {my_city_api_response}.
            Try running the pipeline again with a different city!"""
        )
else:
    st.markdown(
        "Provide a city name in the 'user_input_variables.py' file and run part 1 of the Airflow pipeline to see the current weather in your city."
    )


# --------------------------- #
# Number of hot days per year #
# --------------------------- #

st.subheader(f"Days over {hot_day}°C per year")

if c.REPORT_HISTORICAL_WEATHER_TABLE_NAME in tables:

    if len(historical_weather_table.city.unique()) == 1:
        st.markdown(
            """
            By default the graph below will show hot days per year for the city of Bern.
            Complete exercise 2 to use dynamic task mapping to retrieve historical weather information from list of cities of your choosing that will populate the 
            dropdown menu.
            """
        )


    # define columns
    col1, col2 = st.columns([3, 2])

    # col 1 contains the time-period slider
    with col1:

        # add slider
        timespan = st.slider(
            "Adjust the time-period shown.",
            value=(datetime(1960, 1, 1), datetime(2023, 1, 1)),
            format="YYYY",
        )

        historical_weather_table = historical_weather_table[
            (historical_weather_table["Year"] >= timespan[0])
            & (historical_weather_table["Year"] <= timespan[1])
        ]

    # col 2 contains the check-boxes for global data and user-selected country
    with col2:

        # add selectbox for grain of data to display
        grain = st.selectbox(
            "City",
            historical_weather_table.city.unique(),
            label_visibility="visible",
            index=0,
        )

        selected_city_historical_weather = historical_weather_table[
            historical_weather_table.city == grain
        ]

    st.line_chart(selected_city_historical_weather, x="Year", y="Heat days per year")
    

# ------------------------ #
# Hottest day in birthyear #
# ------------------------ #


if c.REPORT_HOT_DAYS_TABLE_NAME in tables and c.REPORT_HISTORICAL_WEATHER_TABLE_NAME in tables:
    st.subheader(f"Hottest Day in {uv.BIRTH_YEAR}")

    if len(historical_weather_table.city.unique()) != len(hot_days_table):
        st.markdown(
            """
            The table below shows the output of the `find_hottest_day_birthyear` task in the `transform_historical_weather` DAG.
            By default this table will contain historical weather data for the city of Bern. Complete exercise 2 to retrieve data for a list of cities of your choosing and
            transform the table within the `find_hottest_day_birthyear` task to display the hottest day in your birthyear for each city (exercise 3).
            """
        )

    st.write(hot_days_table)
else:
    st.subheader("Hottest Day in your birthyear")
    st.markdown(
        "Run part 2 of the Airflow pipeline to see a graph with historical weather information about the city of Bern. Complete exercise 2 and 3 to customize the graph and table."
    )

if c.REPORT_HISTORICAL_WEATHER_TABLE_NAME in tables and len(
    historical_weather_table.city.unique()
) == len(hot_days_table):
    st.success(f"Congratulations, {user_name}, on finishing this tutorial!", icon="🎉")


# ------- #
# Sidebar #
# ------- #

with st.sidebar:

    # display logos of tools used with links to their websites as well as attribute data sources
    st.markdown(
        """
        <h2> Tools used </h2>
            <a href='https://docs.astronomer.io/astro/cli/install-cli', title='Astro CLI by Astronomer'>
                <img src='https://avatars.githubusercontent.com/u/12449437?s=280&v=4'  width='50' height='50'></a>
            <a href='https://airflow.apache.org/', title='Apache Airflow'>
                <img src='https://pbs.twimg.com/media/EFOe7T4X4AEfIyl.jpg'  width='50' height='50'></a>
            <a href='https://duckdb.org/', title='DuckDB'>
                <img src='https://duckdb.org/images/favicon/apple-touch-icon.png'  width='50' height='50'></a>
            <a href='https://streamlit.io/', title='Streamlit'>
                <img src='https://streamlit.io/images/brand/streamlit-mark-color.svg'  width='50' height='50'></a>
        </br>
        </br>
        <h2> Data sources </h2>
            <a href='https://open-meteo.com/'> Open Meteo API </a>
            </br>
            (<a href='https://creativecommons.org/licenses/by/4.0/'>CC BY 4.0</a>)
        </br>
        </br>
            <a href='https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data'> Berkely Earth and Kristen Sissener </a>
            </br>
            (<a href='https://creativecommons.org/licenses/by-nc-sa/4.0/'>CC BY-NC-SA 4.0</a>)
        </br>
        </br>
        """,
        # warning: using html in your streamlit app can open you to security
        # risk when writing unsafe html code,
        # see: https://github.com/streamlit/streamlit/issues/152
        unsafe_allow_html=True,
    )

    st.button("Re-run")
