FROM coady/pylucene:latest


ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN pip install joblib


WORKDIR /app

COPY ./lucene_part.py /app/
COPY ./object_types.py /app/
COPY ./df/merged_df.tsv /app/df/merged_df.tsv

CMD ["python", "lucene_part.py"]