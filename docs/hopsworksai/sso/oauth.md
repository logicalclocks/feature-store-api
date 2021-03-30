# Configure your hopsworks cluster to use OAuth2 for user management.
Once you have created a Hopsworks cluster you can configure it to use OAuth2 for its access control.
We will go through a step-by-step description of the configuration process.
To illustrate our explanation We will use Azure Active Directory as the identity provider, but the same can be done with any identity provider supporting OAuth2.

## Step 1: Configure your identity provider.
To use OAuth2 in hopsworks you first need to create and configure an OAuth client in your identity provider. We will take the example of Azure AD for the remaining of this documentation, but equivalent steps can be taken on other identity providers.

Navigate to the [Microsoft Azure Portal](https://portal.azure.com) and authenticate. Navigate to [Azure Active Directory](https://portal.azure.com/#blade/Microsoft_AAD_IAM/ActiveDirectoryMenuBlade/Overview). Click on [App Registrations](https://portal.azure.com/#blade/Microsoft_AAD_IAM/ActiveDirectoryMenuBlade/RegisteredApps). Click on *New Registration*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/create_application.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/create_application.png" alt="Create application">
    </a>
    <figcaption>Create application</figcaption>
  </figure>
</p>

Enter a name for the client such as *hopsworks_oauth_client*. Verify the Supported account type is set to *Accounts in this organizational directory only*. And Click Register.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/name_application.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/name_application.png" alt="Name application">
    </a>
    <figcaption>Name application</figcaption>
  </figure>
</p>

In the Overview section, copy the *Application (client) ID field*. We will use it in [step 2](#step-2-configure-hopsworks) under the name *OAUTH_CLIENT_ID*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/client_id.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/client_id.png" alt="Copy client ID">
    </a>
    <figcaption>Copy client ID</figcaption>
  </figure>
</p>

Click on *Endpoints* and copy the *OpenId Connect metadata document* endpoint excluding the *.well-known/openid-configuration* part. We will use it in [step 2](#step-2-configure-hopsworks) under the name *PROVIDER_URI*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/endpoint.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/endpoint.png" alt="Endpoint">
    </a>
    <figcaption>Endpoint</figcaption>
  </figure>
</p>

Click on *Certificates & secrets*, then Click on *New client secret*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/new_client_secret.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/new_client_secret.png" alt="New client secret">
    </a>
    <figcaption>New client secret</figcaption>
  </figure>
</p>

Add a *description* of the secret. Select an expiration period. And, Click *Add*. 

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/new_client_secret_config.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/new_client_secret_config.png" alt="Client secret creation">
    </a>
    <figcaption>Client secret creation</figcaption>
  </figure>
</p>

Copy the secret. This will be used in [step 2](#step-2-configure-hopsworks) under the name *OAUTH_CLIENT_SECRET*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/copy_secret.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/copy_secret.png" alt="Client secret creation">
    </a>
    <figcaption>Client secret creation</figcaption>
  </figure>
</p>

Click on *Authentication*. Then click on *Add a platform*

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/add_platform.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/add_platform.png" alt="Add a platform">
    </a>
    <figcaption>Add a platform</figcaption>
  </figure>
</p>

In *Configure platforms* click on *Web*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/add_platform_web.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/add_platform_web.png" alt="Configure platform: Web">
    </a>
    <figcaption>Configure platform: Web</figcaption>
  </figure>
</p>

Enter the *Redirect URI* and click on *Configure*. The redirect URI is *HOPSWORKS-URI/callback* with *HOPSWORKS-URI* the URI of your hopsworks cluster. You can find it by going to the [hopsworks.ai dashboard](https://managed.hopsworks.ai/dashboard) in the *General* tab of your cluster and copying the URI (excluding the */#!/*).

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/add_platform_redirect.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/add_platform_redirect.png" alt="Configure platform: Redirect">
    </a>
    <figcaption>Configure platform: Redirect</figcaption>
  </figure>
</p>

## Step 2: Configure Hopsworks
Log into your Hopsworks cluster and go to the admin page.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/hopsworks_admin.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/hopsworks_admin.png" alt="Hopsworks admin">
    </a>
    <figcaption>Hopsworks admin</figcaption>
  </figure>
</p>

Click on *Edit variables*

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/edit_variables.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/edit_variables.png" alt="Edit variables">
    </a>
    <figcaption>Edit variables</figcaption>
  </figure>
</p>

Enter *oauth* in the *Name* entry field (1). Set *oauth_enabled* to *true* (2).Set *oauth_redirect_uri* to the same redirect URI as above (3). Set *oauth_logout_redirect_uri* to the same redirect URI as above without the *callback* at the end (4). Set *oauth_account_status* to 2 (5). Set *oauth_group_mapping* to *ANY_GROUP->HOPS_USER* (6). Click on *Reload variables* (7) and click on *Admin Home* (8). 

!!! Note
        If you let the value 1 for *oauth_account_status* an administrator will need to enable the user in hopsworks each time a new user tries to login with OAuth. 
        
!!! Note
    Setting *oauth_group_mapping* to *ANY_GROUP->HOPS_USER* will assign the role *user* to any user from any group in your identity provider when the log into hopsworks with OAuth for the first time. You can replace *ANY_GROUP* with the group of your choice in the identity provider. You can replace *HOPS_USER* by *HOPS_ADMIN* if you want the users of the group to be admins in hopsworks. You do several mappings of groups to roles by comma separating them.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/set_variables.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/set_variables.png" alt="Set variables">
    </a>
    <figcaption>Set variables</figcaption>
  </figure>
</p>

Click on *Register OAuth 2.0 Client*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/register_client.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/register_client.png" alt="Register OAuth 2.0 Client">
    </a>
    <figcaption>Register OAuth 2.0 Client</figcaption>
  </figure>
</p>

Click on the *+* next to *Register OpenId provider and client* (1). Set *Client id* to be the *OAUTH_CLIENT_ID* you copied above (2).  Set *Client secret* to be the *OAUTH_CLIENT_SECRET* you copied above (3). Give a name to your provider in *Provider name* for example, *OAuth* (4). Give the name that will be displayed on the login page for your provider in *Provider display name* for example, *OAuth* (5). Set *Provider URI* to the *PROVIDER_URI* you copied above (6). Click *Save* (7).

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/configure_client.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/configure_client.png" alt="Configure OAuth 2.0 Client">
    </a>
    <figcaption>Configure OAuth 2.0 Client</figcaption>
  </figure>
</p>

Users will now see a new button on the login page. The button has the name you set above for *Provider display name* and will redirect to your identity provider.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/sso/login.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/sso/login.png" alt="Login">
    </a>
    <figcaption>Login</figcaption>
  </figure>
</p>

