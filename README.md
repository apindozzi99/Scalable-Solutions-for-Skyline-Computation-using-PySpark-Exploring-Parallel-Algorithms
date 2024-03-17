# Scalable-Solutions-for-Skyline-Computation-using-PySpark-Exploring-Parallel-Algorithms
Algorithms for the parallel computation of the skyline (master thesis).

Here's a synthetic and clear pointed list of the objectives and contributions of this thesis.

• Comprehensive Exploration of existing methodologies in the parallel computing domain, focusing on state-of-the-art applications tailored to skyline computation.
• Efficiency Evaluation of parallel computing tools across various experimental setups
• Empirical Assessment of Optimization Strategies aimed at optimizing the performance of skyline computation algorithms in a parallel computing environment.
• Rigorously validate the proposed approaches using a diverse set of datasets, encompassing both synthetic and real-world scenarios
• Ensure the robustness and applicability of the developed approach in practical, varied contexts through the validation process.
• Identify potential avenues for further improvement based on the experimental results.

The experiments were conducted on a robust computational infrastructure comprising four virtual machines, each equipped with 30 cores and 8GB of RAM. These machines are interconnected via a Spark cluster, enabling us to leverage the collective computational power of 120 cores and over 30GB of RAM for executing parallel computations.chat

# Spark Environment Setup

This guide will walk you through the installation and configuration of Apache Spark on your system.

## Prerequisites

Before installing Spark, ensure you have the following prerequisites installed on your system:

- Python 3
- pip
- Java

## Installation Steps

1. Download Spark:
   
   ```bash
   wget https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
   ```

2. Extract Spark:

   ```bash
   tar xvf spark-3.1.1-bin-hadoop3.2.tgz
   ```

3. Move Spark to the desired location (e.g., /opt/spark):

   ```bash
   sudo mv spark-3.1.1-bin-hadoop3.2/ /opt/spark
   ```

4. Update environment variables:

   ```bash
   sudo vim ~/.bashrc
   ```

   Add the following lines to the file, then save and exit:

   ```bash
   export SPARK_HOME=/opt/spark
   export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
   ```

   Run the following command to activate tha changes:

   ```bash
   source ~/.bashrc
   ```

## Configuration

### Master Node Setup

1. Navigate to the Spark configuration directory:

   ```bash
   cd /opt/spark/conf
   ```

2. Edit the `spark-env.sh` file:

   ```bash
   sudo vim spark-env.sh
   ```

   Set the `SPARK_MASTER_HOST` variable to your machine's IP address.

3. Start the Spark Master:

   ```bash
   start-master.sh
   ```

4. Verify the Master node by accessing the Spark web interface at:

   http://localhost:8080

### Worker Nodes Setup

1. Start the Worker nodes:

   ```bash
   start-worker.sh spark://<master-ip>:<port>
   ```

### Firewall Configuration

1. Open port 7077 for Spark communication:

   ```bash
   sudo ufw allow 7077
   ```

   Verify the firewall status:

   ```bash
   sudo ufw status
   ```

### Additional Configuration (if needed)

If additional configuration is required, you can modify the `spark-defaults.conf` file:

```bash
sudo vim /opt/spark/conf/spark-defaults.conf
```

Add the following lines to specify executor and driver memory settings:

```
spark.executor.memory <memory-you-need>g
spark.driver.memory <memory-you-need>g
```

Replace `<memory-you-need>` with the desired memory size in gigabytes.

## Running Experiments

To run the experiments, ensure you have Jupyter Notebook installed along with the necessary libraries. You can find the required libraries in the file libraries.ipynb.

The experiments are located in the Experiments folder. Pay attention to the directory of the files required among the machines to allow parallel execution of the code.



