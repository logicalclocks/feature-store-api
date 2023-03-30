package com.logicalclocks.hsfs.beam.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class TCoder<T> extends CustomCoder<T> {
  private static final ObjectMapper mapper = new ObjectMapper();
  private StringUtf8Coder stringCoder = StringUtf8Coder.of();
  private Class<T> type;

  public static <T> TCoder<T> of(Class<T> type) {
    return new TCoder(type);
  }

  public TCoder(Class<T> type) {
    this.type = type;
  }

  public void encode(T value, OutputStream out) throws IOException {
    String data = mapper.writeValueAsString(value);
    stringCoder.encode(data, out);
  }

  public T decode(InputStream in) throws IOException {
    String data = stringCoder.decode(in);
    return mapper.readValue(data, type);
  }

  public void verifyDeterministic() throws Coder.NonDeterministicException {
    throw new Coder.NonDeterministicException(this, "not determistic");
  }
}
