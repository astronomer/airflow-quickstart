pip install streamlit
astro dev start -n --wait 5m
streamlit run include/streamlit_app/weather_v_climate_app.py --server.enableWebsocketCompression=false --server.enableCORS=false --server.enableXsrfProtection=false