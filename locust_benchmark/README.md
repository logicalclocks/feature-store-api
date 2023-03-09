## Online Feature Store Benchmark Suite

This directory provides the tools to quickly bootstrap the process of benchmarking the Hopsworks Online Feature Store.

The benchmark suite is built around [locust](https://locust.io/) which allows you to quickly start so-called user classes to perform requests against a system.

In the feature store context, a user class represents a serving application performing requests against the online feature store, looking up feature vectors.

Locust takes care of the hard part of collecting the results and aggregating them.

### Quickstart: Single process locust

By default, each user class gets spawned in its own Python thread, which means, when you run locust in a single Python process, all user classes will share the same core. This is due to the Python Global Interpreter lock.

This section illustrates how to quickly run locust in a single process, see the next section on how to scale locust with a master and multiple worker processes.

#### Prerequisites

1. Hopsworks cluster
   1. You can follow the documentation on how to setup a cluster on [managed.hopsworks.ai](https://docs.hopsworks.ai/3.1/setup_installation/aws/cluster_creation/)
   2. Ideally you create an extra node in the same VPC as the Hopsworks cluster, which you can use to run the benchmark suite from. The easiest way to do this is by using a dedicated [API node](https://docs.hopsworks.ai/3.1/setup_installation/common/rondb/#api-nodes).
   3. Once the cluster is created you need the following information:
      - Hopsworks domain name: **[UUID].cloud.hopsworks.ai** which you can find in the *Details* tab of your managed.hopsworks.ai control plane
      - A project and its name on the hopsworks cluster. For that log into your cluster and create a project.
      - An API key for authentication. Head to your project and [create an API key for your account](https://docs.hopsworks.ai/3.1/user_guides/projects/api_key/create_api_key/).
2. Python environment
   In order to run the benchmark suite, you need a Python environment.
   For that we will SSH into the created API node. Make sure that [SSH is enabled](https://docs.hopsworks.ai/3.1/setup_installation/common/services/) on your cluster.

   1. On AWS all nodes have public IPs, so you can head to your cloud console to find the IP of the API node and SSH directly:
      `ssh ubuntu@[ip-of-api-node] -i your-ssh-key.pem`
      On Azure, you will have to proxy through the Hopsworks head node, you can either use the public IP or the Hopsworks domain name as noted above.
      On GCP, we don't attach SSH keys, but you can use the GCP CLI to create an ssh session.

   2. API nodes come with Anaconda pre-installed, which we can use to create a python environment and install our dependencies:

      ```bash
      conda init bash
      ```

      After this you need to create a new SSH session.

      ```bash
      conda create -n locust-benchmark python=3.9
      conda activate locust-benchmark
      ```

      Clone the feature-store-api repository to retrieve the benchmark suite and install the requirements.
      Make sure to checkout the release branch matching the Hopsworks version of your cluster.

      ```bash
      git clone https://github.com/logicalclocks/feature-store-api.git
      cd feature-store-api
      git checkout master
      cd locust_benchmark
      pip install -r requirements.txt
      ```

#### Creating feature group for lookups

1. Save the previously created API key in a file named `.api_key`

```bash
echo "[YOUR KEY]" > .api_key
```

2. Now we need to configure the test, for that modify the `hopsworks_config.json` template:

```json
{
    "host": "[UUID].cloud.hopsworks.ai",
    "port": 443,
    "project": "test",
    "external": false,
    "rows": 100000,
    "schema_repetitions": 1,
    "recreate_feature_group": true,
    "batch_size": 100
}
```

- `host`: Domain name of your Hopsworks cluster.
- `port`: optional, for managed.hopsworks.ai it should be `443`.
- `project`: the project to run the benchmark in.
- `external`: if you are not running the benchmark suite from the same VPC as Hopsworks, set this to `true`.
- `rows`: Number of rows/unique primary key values in the feature group used for lookup benchmarking.
- `schema_repetitions`: This controls the number of features for the lookup. One schema repetition will result in 10 features plus primary key. Five repetitions will result in 50 features plus primary key.
- `recreate_feature_group`: This controls if the previous feature group should be dropped and recreated. Set this to true when rerunning the benchmark with different size of rows or schema repetitions.
- `batch_size`: This is relevant for the actual benchmark and controls how many feature vectors are looked up in the batch benchmark.

3. Create the feature group

```bash
python create_feature_group.py
```

Note, the `recreate_feature_group` only matters if you rerun the `create_feature_group.py` script.

#### Run one process locust benchmark

You are now ready to run the load test:
```bash
locust -f locustfile.py --headless -u 4 -r 1 -t 30 -s 1 --html=result.html
```

Options:
- `u`: number of users sharing the python process
- `r`: spawn rate of the users in seconds, it will launch one user every `r` seconds until there are `u` users
- `t`: total time to run the test for
- `s`: shutdown timeout, no need to be changed
- `html`: path for the output file

You can also run only single feature vector or batch lookups by running only the respective user class:

```bash
locust -f locustfile.py FeatureVectorLookup --headless -u 4 -r 1 -t 30 -s 1 --html=result.html

locust -f locustfile.py FeatureVectorBatchLookup --headless -u 4 -r 1 -t 30 -s 1 --html=result.html
```

### Distributed Locust Benchmark using Docker Compose

As you will see already with 4 users, the single core tends to be saturated and locust will print a warning.



```
[2023-03-09 12:02:11,304] ip-10-0-0-187/WARNING/root: CPU usage above 90%! This may constrain your throughput and may even give inconsistent response time measurements! See https://docs.locust.io/en/stable/running-distributed.html for how to distribute the load over multiple CPU cores or machines
```

First we need to install docker, for which we will use [the provided convenience script](https://docs.docker.com/engine/install/ubuntu/#install-using-the-convenience-script), however, you can use any other method too:
```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
```

#### Load the docker image

```bash
wget https://repo.hops.works/dev/moritz/locust_benchmark/locust-2.15.0-hsfs-3.2.0.dev1-amd64.tgz
sudo docker load < locust-2.15.0-hsfs-3.2.0.dev1-amd64.tgz
```

#### Create feature group and configure test

See the above section to setup the `hopsworks_config.json` and create the feature group to perform lookups against.

#### Run multiple locust processes using docker compose

Using docker compose we can now start multiple containers, one dedicated master process, which is responsible for collecting metrics and orchestrating the benchmark, and a variable number of worker processes.

```bash
sudo docker compose up
```

Similarly to the configuration of a single locust process, you have the possibility to configure the test using docker compose in the `docker-compose.yml` file.

Locust command: modify the locust command in the same way as the parameters are described above.

```yml
   command: -f /home/locust/locustfile.py --master --headless --expect-workers 4 -u 16 -r 1 -t 30 -s 1 --html=/home/locust/result.html
```

- `--expect-workers`: this is the only option that's new with this setup and let's you control for how many worker containers locust will wait to be up and running before starting the load test
- `u`: the number of users, is the total number of users which will be distributed evenly among the worker processes. So with 4 workers and 16 users, each worker will run 4 users.

By default, docker compose will launch 4 worker instances, however, you can scale number of workers with

```bash
sudo docker compose up --scale worker=6
```

However, note that the number of workes shouldn't be lower than the `expect-workers` parameter.
As a rule of thumb, you should be running one worker per core of your host.

#### Collect the results

By default, the directory with the test configuration will be mounted in the containers, and locust will create the `result.html` in the mounted directory, so you will be able to access it after the containers are shut down and the test concluded.
