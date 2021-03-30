# Adding and removing workers
Once you have started a hopsworks cluster you can add and remove workers from the cluster to accommodate your workload.

## Adding workers
If the computation you are running is using all the resources of your Hopsworks cluster you can add workers to your cluster.
To add workers to a cluster, go to the *Details* tab of this cluster and click on *Add workers*.

<p align="center">
  <figure>
    <a  href="../../assets/images/hopsworksai/add-worker.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/add-worker.png" alt="Add worker">
    </a>
    <figcaption>Add worker</figcaption>
  </figure>
</p>

Select the number of workers you want to add (1). Select the type of instance you want the workers to run on (2). Select the local storage size for the workers (3). Click on *Next*.

<p align="center">
  <figure>
    <a  href="../../assets/images/hopsworksai/add-workers-config.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/add-workers-config.png" alt="Add workers config">
    </a>
    <figcaption>Add workers</figcaption>
  </figure>
</p>

Review your request and click *Add*.

<p align="center">
  <figure>
    <a  href="../../assets/images/hopsworksai/add-workers-review.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/add-workers-review.png" alt="Add workers review">
    </a>
    <figcaption>Add workers</figcaption>
  </figure>
</p>

Hopsworks.ai will start the new workers and you will be able to use them in your cluster as soon as they have finished starting.

## Removing workers

If the load on your Hopsworks cluster is low, you can decide to remove worker nodes from your cluster.

!!! warning
    When removing workers Hopsworks.ai will try to select workers that can be removed while interfering as little as possible with any ongoing computation. It will also wait for the workers to be done with their computation before stopping them. But, if this computation lasts too long, the worker may get stopped before the computation properly finish. This could interfere with your ongoing computation.

!!! note
    You can remove all the workers of your cluster. If you do so the cluster will be able to store data but not run any computations. This may affect feature store functionality.

To remove workers from a cluster, go to the *Details* tab of this cluster and click on *Remove workers*

<p align="center">
  <figure>
    <a  href="../../assets/images/hopsworksai/remove-worker.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/remove-worker.png" alt="Remove worker">
    </a>
    <figcaption>Remove workers</figcaption>
  </figure>
</p>

For each of the types of instances existing in your cluster select the number of workers you want to remove and click on *Next*.

<p align="center">
  <figure>
    <a  href="../../assets/images/hopsworksai/remove-worker-config.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/remove-worker-config.png" alt="Remove workers config">
    </a>
    <figcaption>Remove workers</figcaption>
  </figure>
</p>

Review your request and click *Remove*.

<p align="center">
  <figure>
    <a  href="../../assets/images/hopsworksai/remove-workers-review.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/remove-workers-review.png" alt="Remove workers review">
    </a>
    <figcaption>Remove workers</figcaption>
  </figure>
</p>

Hopsworks.ai will select the workers corresponding to your criteria which can be stopped with as little interferences as possible with any ongoing computation. It will set them to decommission and stop them when they have finished decommissioning.
