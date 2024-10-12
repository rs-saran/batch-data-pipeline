####################################################################################################################
# Setup containers to run Airflow & MinIO & Spark

docker-compose-up:
	docker compose build && docker compose up airflow-init && docker compose up --build -d 
setup-conn:
	docker exec scheduler python /opt/airflow/setup_conn.py

do-wait:
	sleep 30

up: docker-compose-up do-wait setup-conn

down:
	docker compose down

restart: down up

sh:
	docker exec -it webserver bash



# Testing, auto formatting, type checks, & Lint checks
pytest:
	docker exec webserver pytest -p no:warnings -v /opt/airflow/tests

format:
	docker exec webserver python -m black -S --line-length 79 .

isort:
	docker exec webserver isort .

type:
	docker exec webserver mypy --ignore-missing-imports /opt/airflow

lint: 
	docker exec webserver flake8 /opt/airflow/dags

ci: isort format type lint pytest