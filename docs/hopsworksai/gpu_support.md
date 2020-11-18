# GPU support
Hopsworks can harness the power of GPUs to speed up machine learning processes. You can take advantage of this feature in Hopsworks.ai by adding GPU equipped workers to your cluster. This can be done in two way: creating a cluster with GPU equipped workers or adding GPU equipped workers to an existing cluster.

!!! warning
    This feature is not supported yet in Azure but will be added shortly.

## Creating a cluster with GPU equipped workers
When selecting the [workers' instance type](aws/cluster_creation.md#step-2-setting-the-general-information) during the cluster creation, you can select an instance type equipped with GPUs. The cluster will then be created and Hopsworks will automatically detect the GPU resource.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/create-gpu.png">
      <img src="../../../assets/images/hopsworksai/create-gpu.png" alt="Create cluster with GPUs">
    </a>
    <figcaption>Create cluster with GPUs</figcaption>
  </figure>
</p>

## Adding GPU equipped workers to an existing cluster.
When [adding workers](adding_removing_workers.md#adding-workers) to a cluster, you can select an instance type equipped with GPUs. The workers will then be added to the cluster and Hopsworks will automatically detect the new GPU resource.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/add-gpu.png">
      <img src="../../../assets/images/hopsworksai/add-gpu.png" alt="Add GPUs to cluster">
    </a>
    <figcaption>Add GPUs to cluster</figcaption>
  </figure>
</p>

## Using the GPUs
Once workers with GPUs have been added to your cluster you can use them by allocating GPUs to JupyterLab or Jobs.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/gpu-jupyter.png">
      <img src="../../../assets/images/hopsworksai/gpu-jupyter.png" alt="Using GPUs in JupyterLab">
    </a>
    <figcaption>Using GPUs in JupyterLab</figcaption>
  </figure>
</p>

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/gpu-jobs.png">
      <img src="../../../assets/images/hopsworksai/gpu-jobs.png" alt="Using GPUs in jobs">
    </a>
    <figcaption>Using GPUs in jobs</figcaption>
  </figure>
</p>

For more information about using GPUs in Hopsworks you can consult Hopsworks [Experiments documentation](https://hopsworks.readthedocs.io/en/stable/hopsml/experiment.html).