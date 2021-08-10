/*
 * Copyright (c) 2021 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs.metadata;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
public class User {

  @Getter
  @Setter
  private String username;

  @Getter
  @Setter
  private String email;

  @Getter
  @Setter
  private String firstName;

  @Getter
  @Setter
  private String lastName;

  @Getter
  @Setter
  private int status;

  @Getter
  @Setter
  private String secret;

  @Getter
  @Setter
  private String chosenPassword;

  @Getter
  @Setter
  private String repeatedPassword;

  @Getter
  @Setter
  private boolean tos;

  @Getter
  @Setter
  private boolean twoFactor;

  @Getter
  @Setter
  private int toursState;

  @Getter
  @Setter
  private int maxNumProjects;

  @Getter
  @Setter
  private int numCreatedProjects;

  @Getter
  @Setter
  private boolean testUser;

  @Getter
  @Setter
  private String userAccountType;

  @Getter
  @Setter
  private int numActiveProjects;

  @Getter
  @Setter
  private int numRemainingProjects;
}
