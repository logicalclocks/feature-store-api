window.addEventListener("DOMContentLoaded", function () {

    var windowPathNameSplits = window.location.pathname.split("/");
    var majorVersionRegex = new RegExp("(\\d+[.]\\d+)")

    if (majorVersionRegex.test(windowPathNameSplits[1])) { // On landing page docs.hopsworks.api/3.0 - URL contains major version

        document.getElementById("hopsworks_api_link").href = "https://docs.hopsworks.ai/hopsworks-api/" + windowPathNameSplits[1];
        document.getElementById("hsfs_api_link").href = "https://docs.hopsworks.ai/feature-store-api/" + windowPathNameSplits[1];
        document.getElementById("hsml_api_link").href = "https://docs.hopsworks.ai/machine-learning-api/" + windowPathNameSplits[1];

    } else { // on docs.hopsworks.api/feature-store-api/3.0 / docs.hopsworks.api/hopsworks-api/3.0 / docs.hopsworks.api/machine-learning-api/3.0
        var apiVersion = windowPathNameSplits[2];
        var majorVersion = apiVersion.match(majorVersionRegex)[0];

        document.getElementsByClassName("md-tabs__link")[0].href = "https://docs.hopsworks.ai/" + majorVersion;
        document.getElementsByClassName("md-tabs__link")[1].href = "https://docs.hopsworks.ai/" + majorVersion + "/getting_started/quickstart/";
        document.getElementsByClassName("md-tabs__link")[2].href = "https://docs.hopsworks.ai/" + majorVersion + "/tutorials/fraud_batch/1_feature_groups/";
        document.getElementsByClassName("md-tabs__link")[3].href = "https://docs.hopsworks.ai/" + majorVersion + "/concepts/hopsworks/";
        document.getElementsByClassName("md-tabs__link")[4].href = "https://docs.hopsworks.ai/" + majorVersion + "/user_guides/"
        document.getElementsByClassName("md-tabs__link")[5].href = "https://docs.hopsworks.ai/" + majorVersion; + "/setup_installation/aws/getting_started/"
        document.getElementsByClassName("md-tabs__link")[6].href = "https://docs.hopsworks.ai/" + majorVersion; + "/admin/"
    }
});
