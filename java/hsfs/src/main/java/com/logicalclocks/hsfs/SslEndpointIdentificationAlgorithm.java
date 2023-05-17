/*
 *  Copyright (c) 2021-2023. Hopsworks AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonCreator;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import java.util.Arrays;
import java.util.Optional;

@XmlEnum
public enum SslEndpointIdentificationAlgorithm {
  @XmlEnumValue("HTTPS")
  HTTPS("HTTPS"),
  @XmlEnumValue("")
  EMPTY("");

  private String value;

  SslEndpointIdentificationAlgorithm(String value) {
    this.value = value;
  }

  @JsonCreator
  public static SslEndpointIdentificationAlgorithm fromString(String value) {
    Optional<SslEndpointIdentificationAlgorithm> algorithm = Arrays.stream(SslEndpointIdentificationAlgorithm.values())
        .filter(a -> a.getValue().equals(value.toUpperCase())).findFirst();

    if (algorithm.isPresent()) {
      return algorithm.get();
    } else {
      throw new IllegalArgumentException("Invalid ssl endpoint identification algorithm provided");
    }
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  // Overriding toString() method to return "" instead of "EMPTY"
  @Override
  public String toString() {
    return this.value;
  }
}
