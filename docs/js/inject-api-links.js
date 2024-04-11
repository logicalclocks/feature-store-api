window.addEventListener("DOMContentLoaded", function () {
    var windowPathNameSplits = window.location.pathname.split("/");
    var majorVersionRegex = new RegExp("(\\d+[.]\\d+)")
    var latestRegex = new RegExp("latest")
    if (majorVersionRegex.test(windowPathNameSplits[1])) { // On landing page docs.hopsworks.api/3.0 - URL contains major version
        // Version API dropdown
        document.getElementById("hopsworks_api_link").href = "https://docs.hopsworks.ai/hopsworks-api/" + windowPathNameSplits[1] + "/generated/api/login/";
        document.getElementById("hsfs_api_link").href = "https://docs.hopsworks.ai/feature-store-api/" + windowPathNameSplits[1] + "/generated/api/connection_api/";
        document.getElementById("hsml_api_link").href = "https://docs.hopsworks.ai/machine-learning-api/" + windowPathNameSplits[1] + "/generated/connection_api/";
    } else { // on docs.hopsworks.api/feature-store-api/3.0 / docs.hopsworks.api/hopsworks-api/3.0 / docs.hopsworks.api/machine-learning-api/3.0
        if (latestRegex.test(windowPathNameSplits[2]) || latestRegex.test(windowPathNameSplits[1])) {
          var majorVersion = "latest";
        } else {
          var apiVersion = windowPathNameSplits[2];
          var majorVersion = apiVersion.match(majorVersionRegex)[0];
        }
        // Version main navigation
        document.getElementsByClassName("md-tabs__link")[0].href = "https://docs.hopsworks.ai/" + majorVersion;
        document.getElementsByClassName("md-tabs__link")[1].href = "https://colab.research.google.com/github/logicalclocks/hopsworks-tutorials/blob/master/quickstart.ipynb";
        document.getElementsByClassName("md-tabs__link")[2].href = "https://docs.hopsworks.ai/" + majorVersion + "/tutorials/";
        document.getElementsByClassName("md-tabs__link")[3].href = "https://docs.hopsworks.ai/" + majorVersion + "/concepts/hopsworks/";
        document.getElementsByClassName("md-tabs__link")[4].href = "https://docs.hopsworks.ai/" + majorVersion + "/user_guides/";
        document.getElementsByClassName("md-tabs__link")[5].href = "https://docs.hopsworks.ai/" + majorVersion + "/setup_installation/aws/getting_started/";
        document.getElementsByClassName("md-tabs__link")[6].href = "https://docs.hopsworks.ai/" + majorVersion + "/admin/";
        // Version API dropdown
        document.getElementById("hopsworks_api_link").href = "https://docs.hopsworks.ai/hopsworks-api/" + majorVersion + "/generated/api/login/";
        document.getElementById("hsfs_api_link").href = "https://docs.hopsworks.ai/feature-store-api/" + majorVersion + "/generated/api/connection_api/";
        document.getElementById("hsfs_javadoc_link").href = "https://docs.hopsworks.ai/feature-store-api/" + majorVersion + "/javadoc";
        document.getElementById("hsml_api_link").href = "https://docs.hopsworks.ai/machine-learning-api/" + majorVersion + "/generated/connection_api/";
    }
});
