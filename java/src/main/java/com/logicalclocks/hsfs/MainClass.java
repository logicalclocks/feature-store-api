/*
 * Copyright (c) 2020 Logical Clocks AB
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

package com.logicalclocks.hsfs;


import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class MainClass {


  public static void main(String[] args) throws Exception {
    Instant instant = Instant.ofEpochSecond(1605259949L);
    String datetime =
        LocalDateTime.ofInstant(instant, ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    System.out.println(datetime);

    String pattern = "yyyyMMddHHmmss";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
    LocalDateTime localDateTime = LocalDateTime.from(formatter.parse(datetime));
    System.out.println(localDateTime.toEpochSecond(ZoneOffset.UTC));


  }
}
