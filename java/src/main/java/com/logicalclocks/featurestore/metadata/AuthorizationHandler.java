package com.logicalclocks.featurestore.metadata;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;

public class AuthorizationHandler<T> implements ResponseHandler<T> {

  private ResponseHandler<T> originalResponseHandler;

  AuthorizationHandler(ResponseHandler<T> originalResponseHandler) {
    this.originalResponseHandler = originalResponseHandler;
  }

  @Override
  public T handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
    if (response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
      throw new UnauthorizedException();
    } else if (response.getStatusLine().getStatusCode() / 100 == 4) {
      throw new IOException("Error: " + response.getStatusLine().getStatusCode() +
          EntityUtils.toString(response.getEntity(), Charset.defaultCharset()));
    } else if (response.getStatusLine().getStatusCode() / 100 == 5) {
      // TODO(fabio): Propagate http error upstream
      throw new InternalException();
    }

    return originalResponseHandler.handleResponse(response);
  }
}
