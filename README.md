# avro-schema-kafka
Stream [Bitcoin Price Training Data](https://www.kaggle.com/datasets/team-ai/bitcoin-price-prediction?select=bitcoin_price_Training+-+Training.csv) on Apache Kafka using Schema Registry as Avro Format sink to BigQuery.

1. Provide your Service Account Key File as '_**creds.json**_' for BigQuery inside [.credentials/](https://github.com/zeenfts/avro-schema-kafka/tree/main/.credentials).
2. `docker compose up` to start everything.
3. `docker logs --follow app-producer` on new terminal. Please look at the end line after 'Start Producer App'.
4. `docker logs --follow app-consumer` on new terminal. Please look at the end line after 'Start Consumer App'.
5. You should see something similar as shown on [imgs/](https://github.com/zeenfts/avro-schema-kafka/tree/main/imgs).


<sub>
Note: <br>
- You can also check on the <a href="http://localhost:9021">Kafka Control Center</a> for better UI! <br>
- Don't forget to end up everything <code>docker container stop $(docker ps -a -q); docker system prune -f; docker volume prune -f; docker container prune -f</code>. <br>Because the data stream to the BigQuery, unless you have no worry for the <span style="color:red">Billing</span>!
</sub>
