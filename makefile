data-stream:
	spark-submit --verbose --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 data_stream.py \
	--conf "spark.default.paralelism=16"

kafka-consumer:
	kafka-console-consumer --topic "sf_crime" --from-beginning --bootstrap-server localhost:9092

kafka-consumer-py:
	python3 consumer_server.py

kafka-cluster:
	cd clojure && clojure -m com.dph.kafka

kafka-server:
	python3 kafka_server.py
