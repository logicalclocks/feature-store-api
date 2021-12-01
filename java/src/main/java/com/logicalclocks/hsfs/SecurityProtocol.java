package com.logicalclocks.hsfs;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;

@XmlEnum
public enum SecurityProtocol {
  @XmlEnumValue("PLAINTEXT")
  PLAINTEXT,
  @XmlEnumValue("SASL_PLAINTEXT")
  SASL_PLAINTEXT,
  @XmlEnumValue("SASL_SSL")
  SASL_SSL,
  @XmlEnumValue("SSL")
  SSL;
}
