build:
	docker compose build
	
build-nc:
	docker compose build --no-cache

build-progress:
	docker compose build --no-cache --progress=plain

down:
	docker compose down --volumes --remove-orphans

run:
	make down && docker compose up

run-scaled:
	make down && docker compose up --scale spark-worker=3

run-d:
	make down && docker compose up -d

stop:
	docker compose stop

submit:
	docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)

checkdb:
	docker exec da-spark-master python /opt/spark/apps/predictprice/check_db.py

submitmain:
	docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --py-files /opt/spark/apps/predictprice/scrapers.zip,/opt/spark/apps/predictprice/config.py,/opt/spark/apps/predictprice/ingestion.py /opt/spark/apps/predictprice/main.py