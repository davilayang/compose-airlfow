# Airflow with Docker Compose

> apache-airflow version 1.10.14

## Steps

### 1. Get and edit the yaml file

Download the yaml file, and edit the image version to `apache/airflow:1.10.14-python3.6`

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.0.1/docker-compose.yaml'

mkdir ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

Or, replace with a Dockerfile in the same directory, which will build a custom image by installing from `requirements.txt`

> The service needs a custom image from the official one with PYPI packages from `requirements.txt`. This can be done by creating a Dockerfile and then building the image with `docker build . -t my-airflow`, and edit yaml file to point the _image_ key to `my-airflow`. Or just edit the yaml file to replace _image_ key with `build: .`, which points to the Dockerfile in the same directory.  

### 2. Initialize the backend

Setup the directories and environment variables. Also initialize the database and message broker.  

```bash
docker-compose build
docker-compose up airflow-init
```

> There was a problem with the yaml from documentation, where `airflow-init` doesn't really initialize the databases. It only created the database containers. This was fixed by changing the `command` section to `init db` instead of `version`. Alternatively, run `docker-compose run airflow-webserver db init` after `docker-compose up airflow-init` will also initialize the databases.  

### 3. Start Airflow

```bash
docker-compose up
docker-compose up -d # run in daemon mode
```

Visit the airflow webserver at http://localhost:8080/

### 4. Remove all installations

Stop the running services and remove volumes

```bash
docker-compose down --volumes
```

## Others

To inspect the airflow image by exec into it, overriding the entrypoint

```bash
# interactive bash
docker run --rm -it --entrypoint "/bin/bash" apache/airflow:1.10.14-python3.6
# ls -al on the workdir only
docker run --rm --entrypoint "/bin/bash" apache/airflow:1.10.14-python3.6 ls -al
```

View container status

```bash
docker-compose ps
docker ps
```

## References

+ [Running Airflow in Docker (2.0.1)](https://airflow.apache.org/docs/apache-airflow/2.0.1/start/docker.html)
+ [Set up a Database Backend](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html)
+ [Compose file version 3 reference](https://docs.docker.com/compose/compose-file/compose-file-v3/)