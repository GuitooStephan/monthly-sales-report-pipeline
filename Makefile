# Migrate the database and create the user
build:
	docker compose up airflow-init

# Run all the services including airflow
run:
	docker compose up

# Set up test environment
# You can change the uri to your own postgresql database
setup-test-env:
	airflow db init
	airflow connections add 'postgres_db' --conn-uri postgresql://airflow:airflow@localhost:5433/airflow

# Run test
test:
	PYTHONPATH=./airflow/dags pytest 