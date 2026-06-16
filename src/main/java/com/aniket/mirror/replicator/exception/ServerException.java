package com.aniket.mirror.replicator.exception;

import org.springframework.http.HttpStatus;

public class ServerException extends AppException {
  public ServerException(String errorCode, HttpStatus status, String message) {
    super(errorCode, status, message);
  }

  public ServerException(String errorCode, HttpStatus status, String message, Throwable cause) {
    super(errorCode, status, message, cause);
  }
}