FROM coady/pylucene:latest

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Note: Base image already has Java configured for PyLucene

# Install PySpark and other useful packages
# RUN pip install --no-cache-dir \
#     pyspark==3.5.0 \
#     pandas \
#     numpy \
#     pyarrow \
#     findspark \
#     joblib

RUN pip install pandas
RUN pip install joblib


# Set working directory
WORKDIR /app

COPY ./lucene_part.py /app/
COPY ./object_types.py /app/
COPY ./left_merged.joblib /app/

# Copy your scripts if any
# COPY ./scripts /app/scripts

# Default command (can be overridden)
CMD ["python", "lucene_part.py"]