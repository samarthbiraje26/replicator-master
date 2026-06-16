package com.aniket.mirror.replicator.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.*;
import org.springframework.web.client.RestClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import lombok.extern.slf4j.Slf4j;

@Configuration
public class RestClientConfig {

  @Bean
  public RestClient.Builder restClientBuilder() {
    HttpComponentsClientHttpRequestFactory apacheFactory = new HttpComponentsClientHttpRequestFactory();
    apacheFactory.setConnectionRequestTimeout(10_000);
    apacheFactory.setReadTimeout(10_000);

    // Buffering needed so response body can be read in interceptor + downstream code
    ClientHttpRequestFactory bufferingFactory =
        new BufferingClientHttpRequestFactory(apacheFactory);

    return RestClient.builder()
        .requestFactory(bufferingFactory)
        .requestInterceptor(new Slf4jLoggingInterceptor());
  }

  @Slf4j
  static class Slf4jLoggingInterceptor implements ClientHttpRequestInterceptor {

    @Override
    public ClientHttpResponse intercept(
        HttpRequest request,
        byte[] body,
        ClientHttpRequestExecution execution
    ) throws IOException {
      long start = System.currentTimeMillis();
      log.info("HTTP OUT -> {} {}", request.getMethod(), request.getURI());
      if (log.isDebugEnabled()) {
        log.debug("HTTP OUT headers={}", request.getHeaders());
        log.debug("HTTP OUT body={}", truncate(body));
      }

      ClientHttpResponse response = execution.execute(request, body);

      byte[] responseBody = response.getBody().readAllBytes();

      long durationMs = System.currentTimeMillis() - start;
      log.info("HTTP OUT <- {} {} ({} in {}ms)", request.getMethod(), request.getURI(), response.getStatusCode(), durationMs);
      if (log.isDebugEnabled()) {
        log.debug("HTTP IN headers={}", response.getHeaders());
        log.debug("HTTP IN body={}", truncate(responseBody));
      }

      // Return a new response so downstream code can still read body
      return new CachedBodyClientHttpResponse(response, responseBody);
    }

    private static String truncate(byte[] body) {
      if (body == null || body.length == 0) {
        return "<empty>";
      }
      String text = new String(body, StandardCharsets.UTF_8);
      return text.length() > 1000 ? text.substring(0, 1000) + "..." : text;
    }
  }

  static class CachedBodyClientHttpResponse implements ClientHttpResponse {

    private final ClientHttpResponse original;
    private final byte[] cachedBody;

    CachedBodyClientHttpResponse(ClientHttpResponse original, byte[] cachedBody) {
      this.original = original;
      this.cachedBody = cachedBody;
    }

    @Override
    public InputStream getBody() {
      return new ByteArrayInputStream(cachedBody);
    }

    @Override
    public org.springframework.http.HttpHeaders getHeaders() {
      return original.getHeaders();
    }

    @Override
    public org.springframework.http.HttpStatusCode getStatusCode() throws IOException {
      return original.getStatusCode();
    }

    @Override
    public String getStatusText() throws IOException {
      return original.getStatusText();
    }

    @Override
    public void close() {
      original.close();
    }
  }
}
