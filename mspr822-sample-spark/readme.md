# Interactive mode

# Run spark

```bash
docker run --name spark-app --rm -it -v "$PWD:/tmp/dataset/" spark:latest /opt/spark/bin/pyspark
```


# Load data into spark


```python
events = spark.read.option('header', True).csv(list(map(lambda x: f'/tmp/dataset/{x}', os.listdir('/tmp/dataset/'))))
```

# Daemon mode

## Submit app

```bash
docker run --name spark-app-mspr822 --network mspr822_default --rm  -v "$PWD:/tmp/dataset/" spark:latest /opt/spark/bin/spark-submit --master spark://nodemain:7077 /tmp/dataset/msprapp.py
```