package com.aniket.mirror.replicator.exception;

import org.springframework.http.HttpStatus;

public class AppException extends RuntimeException {

  private final String errorCode;
  private final HttpStatus status;

  public AppException(String errorCode, HttpStatus status, String message) {
    super(message);
    this.errorCode = errorCode;
    this.status = status;
  }

  public AppException(String errorCode, HttpStatus status, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
    this.status = status;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public HttpStatus getStatus() {
    return status;
  }
}