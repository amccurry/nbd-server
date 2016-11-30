/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package nbd;

import java.io.IOException;

public abstract class Storage {
  private final String exportName;

  public Storage(String exportName) {
    this.exportName = exportName;
  }

  public final String getExportName() {
    return exportName;
  }

  public abstract void connect() throws IOException;

  public abstract void disconnect() throws IOException;

  public abstract ExecCommand read(byte[] buffer, long offset);

  public abstract ExecCommand write(byte[] buffer, long offset);

  public abstract ExecCommand flush();

  public abstract long size();

}
