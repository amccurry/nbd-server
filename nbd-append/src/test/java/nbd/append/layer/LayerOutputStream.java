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
package nbd.append.layer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.google.common.io.CountingOutputStream;

public class LayerOutputStream implements LayerOutput {

  private final DataOutputStream output;
  private final CountingOutputStream countingOutputStream;

  public LayerOutputStream(OutputStream outputStream) {
    countingOutputStream = new CountingOutputStream(outputStream);
    output = new DataOutputStream(countingOutputStream);
  }

  @Override
  public long getPosition() {
    return countingOutputStream.getCount();
  }

  @Override
  public void write(int b) throws IOException {
    output.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    output.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    output.write(b, off, len);
  }

  @Override
  public final void writeBoolean(boolean v) throws IOException {
    output.writeBoolean(v);
  }

  @Override
  public final void writeByte(int v) throws IOException {
    output.writeByte(v);
  }

  @Override
  public final void writeShort(int v) throws IOException {
    output.writeShort(v);
  }

  @Override
  public final void writeChar(int v) throws IOException {
    output.writeChar(v);
  }

  @Override
  public final void writeInt(int v) throws IOException {
    output.writeInt(v);
  }

  @Override
  public final void writeLong(long v) throws IOException {
    output.writeLong(v);
  }

  @Override
  public final void writeFloat(float v) throws IOException {
    output.writeFloat(v);
  }

  @Override
  public final void writeDouble(double v) throws IOException {
    output.writeDouble(v);
  }

  @Override
  public final void writeBytes(String s) throws IOException {
    output.writeBytes(s);
  }

  @Override
  public final void writeChars(String s) throws IOException {
    output.writeChars(s);
  }

  @Override
  public final void writeUTF(String str) throws IOException {
    output.writeUTF(str);
  }

  @Override
  public void close() throws IOException {
    output.close();
  }
}
