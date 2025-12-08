# Autor: Lubor Koka

# Prva cast

How to run
---

```bash
python ./run.py
```

V `run.py` su volane vsetky funkcie, maximalne su zakomentovane.


# Druha cast

1. Treba prefiltrovat wiki
    ```bash
    python ./filter_wiki.py
    ```

1. Treba vytiahnut data z vyfiltrovanych clankov

    ```bash
    export PYSPARK_PYTHON=$(which python3.11)
    export PYSPARK_DRIVER_PYTHON=$(which python3.11)

    python ./extract_spark.py
    ```
    

1. Spustit lucene index a vyhladavanie

    ```bash
    docker build -t vinf-lucene .
    docker run -it -v . /index:/index vinf-lucene bash
    ```

    v docker bashi potom:

    ```bash
    python lucene_part.py
    ```

