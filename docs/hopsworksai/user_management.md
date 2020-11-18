# User management
In Hopsworks.ai users can be grouped into *organizations* to access the same resources.
When a new user registers with Hopsworks.ai a new organization is created. This user later on can
invite other registered users to their organization so they can share access to the same cluster.

Configuration is also shared among users of the same organization. So if user Alice has configured
her account with her credentials, Bob doesn't have to. Credits and cluster usage are also grouped to
ease reporting.

## Adding members to an organization
Organization membership can be edited my clicking **Members** on the left of Hopsworks.ai Dashboard page.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/members_empty.png">
      <img src="../../../assets/images/hopsworksai/members_empty.png" alt="Organization membership">
    </a>
    <figcaption>Organization membership</figcaption>
  </figure>
</p>

To add a new member to your organization add the user's email and click **Add**

!!! note
     The invited user must have already been registered in Hopsworks.ai

An invited user **must accept** the invitation to be part of the organization. An invitation will show up in
invited member's Dashboard. In this example Alice has invited Bob to her organization, but Bob hasn't accepted
the invitation yet.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/members_invited.png">
      <img src="../../../assets/images/hopsworksai/members_invited.png" alt="Invitation">
    </a>
    <figcaption>Alice has sent the invitation</figcaption>
  </figure>

  <figure>
    <a  href="../../../assets/images/hopsworksai/members_accept.png">
      <img src="../../../assets/images/hopsworksai/members_accept.png" alt="Accept invitation">
    </a>
    <figcaption>Bob's dashboard</figcaption>
  </figure>
</p>

## Sharing resources
Once Bob has accepted the invitation he does **not** have to configure his account, they share the same configuration.
Also, he will be able to view **the same** Dashboard as Alice, so he can start, stop or terminate clusters in the organization.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/alice_dashboard.png">
      <img src="../../../assets/images/hopsworksai/alice_dashboard.png" alt="Alice dashboard">
    </a>
    <figcaption>Alice's dashboard</figcaption>
  </figure>

  <figure>
    <a  href="../../../assets/images/hopsworksai/bob_dashboard.png">
      <img src="../../../assets/images/hopsworksai/bob_dashboard.png" alt="Bob dashboard">
    </a>
    <figcaption>Bob's dashboard</figcaption>
  </figure>
</p>

If Alice had existing clusters running and she has selected [Managed user management](../aws/cluster_creation/#step-9-user-management-selection)
during cluster creation, an account on these clusters will be created for Bob too!

## Removing members from an organization
To remove a member from your organization simply go to **Members** page and click the **Remove** button next to user you want to remove.
You will **stop** sharing any resource and the user **will be blocked** from any shared clusters.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/members_delete.png">
      <img src="../../../assets/images/hopsworksai/members_delete.png" alt="Delete organization member">
    </a>
    <figcaption>Delete organization member</figcaption>
  </figure>
</p>