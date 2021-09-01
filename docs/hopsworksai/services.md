# Services
Hopsworks clusters provide several services that can be accessed from outside Hopsworks. In this documentation, we first show how to make these services accessible to external networks. We will then go through the different services to give a short introduction and link to the associated documentation.

## Outside Access to the Feature Store
By default, only the Hopsworks UI is made available to clients on external networks, like the Internet.
To integrate with external platforms and access APIs for services such as the Feature Store, you have to open the service's ports.

Open ports by going to the *Services* tab, selecting a service, and pressing *Update*. This will update the *Security Group* attached to the Hopsworks cluster to allow incoming traffic on the relevant ports.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/open-ports.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/open-ports.png" alt="Outside Access to the Feature Store">
    </a>
    <figcaption>Outside Access to the Feature Store</figcaption>
  </figure>
</p>

If you do not want the ports to be open to the internet you can set up VPC peering between the Hopsworks VPC and your client VPC. You then need to make sure that the ports associated with the services you want to use are open between the two VPCs. The ports associated with each of the services are indicated in the descriptions of the services below.

## Feature store
The Feature Store is a data management system for managing machine learning features, including the feature engineering code and the feature data. The Feature Store helps ensure that features used during training and serving are consistent and that features are documented and reused within enterprises. You can find the full feature store documentation [here](../index.md) and information about how to connect to the Feature Store from different external services [here](../setup.md)

Ports: 8020, 30010, 9083 and 9085

## Online Feature store
The online Feature store is required for online applications, where the goal is to retrieve a single feature vector with low latency and the same logic as was applied to generate the training dataset, such that the vector can subsequently be passed to a machine learning model in production to compute a prediction. You can find a more detailed explanation of the difference between Online and Offline Feature Store [here](../../../overview/#offline-vs-online-feature-store). Once you have opened the ports, the Online Feature store can be used with the same library as the offline feature store. You can find the full documentation [here](../index.md).

Port: 3306

## Kafka
Hopsworks provides Kafka-as-a-Service for streaming applications and to ingest data. You can find more information about how to use Kafka in hopsworks in this [documentation](https://hopsworks.readthedocs.io/en/stable/user_guide/hopsworks/kafka.html)

Port: 9092

## SSH
If you want to be able to SSH into the virtual machines running the Hopsworks cluster, you can open the ports using the *Services* tab. You can then SSH into the machine using your cluster operation system (*ubuntu* or *centos*) as the user name and the ssh key you selected during the cluster creation.

Port: 22.