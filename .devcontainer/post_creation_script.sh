pip install streamlit
pip install duckdb
astro dev start -n --wait 5m
streamlit run include/streamlit_app.py --server.enableWebsocketCompression=false --server.enableCORS=false --server.enableXsrfProtection=false