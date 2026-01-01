# Big Data Cluster

## Installation Versions

- **Hadoop:** 3.2.1
- **Spark:** 3.3.0

## Web Interface Access

- Hadoop ResourceManager: [http://localhost:8080](http://localhost:8080)
- Hadoop NameNode: [http://localhost:9870](http://localhost:9870)

---

## Deployment System Architecture

- **1 Namenode:** Hadoop's "Master", managing the HDFS file system and YARN resources.
- **3 Datanodes:** Hadoop's "Workers", responsible for actual data storage.
- **1 Spark Master:** Spark's "Master", managing and coordinating tasks.
- **3 Spark Workers:** Spark's "Workers", responsible for executing computation tasks.

> **Note:** To simplify, the Spark Master and Hadoop Namenode will run on the same `master` container.

---

## Starting and Checking the Cluster

**Start the cluster:**
```sh
docker compose up -d
```
### Contact

email: ha.nguyen.fzx@gmail.com | manhhad32@gmail.com

