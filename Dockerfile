FROM quay.io/astronomer/astro-runtime:7.3.0

RUN mkdir -p include/footprint_data
RUN mkdir -p include/historical_weather_data
RUN mkdir -p include/current_weather_data