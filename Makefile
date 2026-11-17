.PHONY: help kafka spark producer validator speed-layer kafka-read stop

# ── Infrastructure ────────────────────────────────────────────────────────────

kafka:
	docker compose -f docker/docker-compose.yml up -d

spark:
	docker compose -f docker/docker-compose.spark.yml up -d

up: kafka spark

stop:
	docker compose -f docker/docker-compose.yml down
	docker compose -f docker/docker-compose.spark.yml down

# ── Services ──────────────────────────────────────────────────────────────────

producer:
	cd $(CURDIR)/producers/experiments/xrpl_producer_sink && python xrpl_to_kafka.py

validator:
	python -m consumers.validator.consumer

speed-layer:
	bash consumers/speed_layer/submit.sh

# ── Inspection ────────────────────────────────────────────────────────────────

kafka-read:
	@read -p "Topic: " topic; \
	docker exec xrpl-kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 \
	  --topic $$topic \
	  --from-beginning

# ── Help ──────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  make up           Start Kafka + Spark"
	@echo "  make kafka        Start Kafka only"
	@echo "  make spark        Start Spark only"
	@echo "  make stop         Stop all containers"
	@echo ""
	@echo "  make producer     Run XRPL → Kafka producer"
	@echo "  make validator    Run validator consumer"
	@echo "  make speed-layer  Submit PySpark speed layer job"
	@echo ""
	@echo "  make kafka-read   Tail xrpl.payments topic from beginning"
	@echo ""
