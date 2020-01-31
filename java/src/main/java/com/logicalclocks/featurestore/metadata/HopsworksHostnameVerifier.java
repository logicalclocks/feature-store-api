package com.logicalclocks.featurestore.metadata;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

public class HopsworksHostnameVerifier implements HostnameVerifier {

  private boolean insecure = false;
  private String hopsworksHost = "";

  public HopsworksHostnameVerifier(boolean insecure, String restEndpoint) {
    this.insecure = insecure;
    this.hopsworksHost = restEndpoint.split(":")[0];
  }

  @Override
  public boolean verify(String string, SSLSession ssls) {
    return true;
    //LOGGER.info("Verifying: " + string);
    //LOGGER.info("against: " + hopsworksHost);
    //return insecure || string.equals(hopsworksHost);
  }
}
