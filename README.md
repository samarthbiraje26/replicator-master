Replicator Service
A distributed file replication microservice for the Multi Mirror Project

Overview
The Replicator Service is an event-driven, asynchronous file replication engine designed to ensure high availability and redundancy across multiple cloud storage providers. Built with enterprise-grade distributed systems principles, it orchestrates parallel file uploads to various mirror providers while maintaining robust error handling, exponential backoff polling, and comprehensive health monitoring.

Key Capabilities
Event-Driven Architecture: Kafka-based message consumption for seamless integration
Multi-Provider Support: Simultaneous replication to multiple storage providers (StreamTape, KrakenFile)
Asynchronous Processing: Non-blocking job execution with Spring's async framework
Intelligent Polling: Exponential backoff strategy with configurable retry limits
Health Monitoring: Scheduled recheck mechanisms for long-term reliability
S3 Integration: Source file management with AWS S3
Observability: Structured logging with MDC context and request tracing
Architecture
System Design
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────────┐
│   File Upload   │─────▶│  Kafka Cluster   │─────▶│  Replicator Service │
│     Service     │      │  (file_upload)   │      │                     │
└─────────────────┘      └──────────────────┘      └──────────┬──────────┘
                                                               │
                         ┌─────────────────────────────────────┤
                         │                                     │
                         ▼                                     ▼
              ┌──────────────────────┐           ┌──────────────────────┐
              │   Job Executor       │           │  Mirror Polling      │
              │   Service            │           │  Scheduler           │
              └──────────┬───────────┘           └──────────┬───────────┘
                         │                                  │
         ┌───────────────┼────────────────┐                 │
         │               │                │                 │
         ▼               ▼                ▼                 ▼
┌─────────────┐  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐
│ StreamTape  │  │ KrakenFile  │  │  Provider N  │  │ Status Check │
│   Provider  │  │  Provider   │  │              │  │   Service    │
└─────────────┘  └─────────────┘  └──────────────┘  └──────────────┘
Multi-Provider Support Architecture
The service implements a Strategy Pattern for provider abstraction:

Base Interface: MirrorProviderService defines upload, poll, and getFileInfo contracts
Provider Implementations:
StreamTapeApiClient: StreamTape API integration
KrakenFileMirrorProviderService: KrakenFile provider
Orchestration: MirrorUploadOrchestratorService coordinates parallel uploads across all enabled providers
State Management: MirrorProvider entity tracks per-provider job status
Provider Selection
Configured via application.yaml:

mirror:
  enabled-providers:
    - STREAM_TAPE
    - KRAKEN_FILE
Event-Driven Workflow
1. File Upload Event Consumption
Kafka Topic: file_upload

Consumer: UploadEventConsumer

Consumes FileUploadEvent messages
Creates FileRecord and FileReplicationJob entities
Triggers async job execution
Event Schema:

{
  "fileId": "uuid",
  "fileName": "document.pdf",
  "contentType": "application/pdf",
  "sizeBytes": 1048576,
  "s3Bucket": "amzn-file-mirror-bucket",
  "s3Key": "uploads/2024/document.pdf",
  "sourceCreatedAt": "2024-01-15T10:30:00Z",
  "checksum": "sha256:abcd1234..."
}
2. Asynchronous Job Execution
Service: JobExecutorService

Retrieves source file metadata from S3
Invokes orchestrator for parallel provider uploads
Updates job status: CREATED → IN_PROGRESS → SUCCESS/FAILED
3. Mirror Upload Orchestration
Service: MirrorUploadOrchestratorService

Identifies enabled providers from configuration
Initiates concurrent uploads to all providers
Aggregates results and publishes success events
Flow:

For each enabled provider:
  ├─▶ Call provider.upload(fileRecord)
  ├─▶ Create MirrorProvider entity (status: UPLOADED)
  └─▶ Save with initial polling metadata
4. Status Polling Mechanism
Scheduler: MirrorPollingScheduler

Cron: Configurable dispatch delay (default: 30 seconds)
Batch Processing: Fetches providers due for polling (batch size: 50)
Exponential Backoff:
Initial: 60 seconds
Max: 1800 seconds (30 minutes)
Max Attempts: 3
Polling Logic:

nextPollAt = lastPolledAt + min(60 * 2^attempt, 1800) seconds
State Transitions:

UPLOADED → ACTIVE (confirmed by provider)
UPLOADED → FAILED (max attempts exceeded)
5. Health Recheck System
Scheduler: MirrorRecheckScheduler

Cron: 0 0 0,12 * * * (midnight and noon daily)
Recheck Period: 30 days
Purpose: Verify long-term file availability
Workflow:

Query ACTIVE providers with lastCheckedAt > 30 days
Call provider.getFileInfo() for each
Update status or mark as FAILED if unavailable
Provider Integration Details
StreamTape API Integration
Base URL: https://api.streamtape.com

Authentication:

Login: ${STREAMTAPE_API_LOGIN} (env variable)
API Key: ${STREAMTAPE_API_KEY} (env variable)
API Endpoints
Endpoint	Method	Purpose
/remotedl/add	GET	Initiate remote download from S3 presigned URL
/remotedl/status	GET	Poll upload status by remoteUploadId
/file/info	GET	Retrieve file metadata by externalFileId
Upload Workflow
Initiate Remote Upload:

GET /remotedl/add?login={login}&key={key}&url={presignedUrl}&name={fileName}
Response: { "id": "remoteUploadId" }

Poll Status (exponential backoff):

GET /remotedl/status?login={login}&key={key}&id={remoteUploadId}
Response:

{
  "status": "finished",
  "url": "https://streamtape.com/v/abc123",
  "id": "abc123"
}
Extract External File ID: Parse id from response (used for health checks)

Error Handling
Retry Logic: 3 attempts with exponential backoff
Failure Scenarios: Network errors, API rate limits, invalid URLs
Logging: Structured error messages with MDC context
KrakenFile Integration
Implementation: KrakenFileMirrorProviderService

Similar workflow to StreamTape
Provider-specific API client (details in service implementation)
Database Schema
Entity Relationship Diagram
┌────────────────────┐
│   FileRecord       │
├────────────────────┤
│ id (PK)            │◀────┐
│ file_id (UUID)     │     │
│ file_name          │     │
│ content_type       │     │
│ size_bytes         │     │ 1:N
│ s3_bucket          │     │
│ s3_key             │     │
│ checksum           │     │
│ source_created_at  │     │
└────────────────────┘     │
                           │
┌────────────────────────┐ │
│ FileReplicationJob     │ │
├────────────────────────┤ │
│ id (PK)                │ │
│ file_record_id (FK)    │─┘
│ status                 │◀────┐
│ created_at             │     │
│ updated_at             │     │
│ completed_at           │     │ 1:N
└────────────────────────┘     │
                               │
┌────────────────────────────┐ │
│ MirrorProvider             │ │
├────────────────────────────┤ │
│ id (PK)                    │ │
│ job_id (FK)                │─┘
│ provider_type              │
│ status                     │
│ remote_upload_id           │
│ external_file_id           │
│ last_polled_at             │
│ next_poll_at               │
│ poll_attempt_count         │
│ last_checked_at            │
│ last_error                 │
└────────────────────────────┘
Key Entities
FileRecord
Purpose: Stores source file metadata
Unique Constraint: (s3_bucket, s3_key)
Index: source_created_at for time-based queries
FileReplicationJob
Purpose: Represents a replication job lifecycle
Status Enum: CREATED, IN_PROGRESS, SUCCESS, FAILED
Cascade: Deletes propagate to MirrorProvider entities
MirrorProvider
Purpose: Tracks individual provider replication state
Provider Type Enum: STREAM_TAPE, KRAKEN_FILE
Status Enum: UPLOADED, ACTIVE, FAILED
Polling Fields:
last_polled_at: Last poll timestamp
next_poll_at: Scheduled next poll time
poll_attempt_count: Current retry attempt
Schema Images
[Placeholder: Add ER diagram image] [Placeholder: Add database schema visualization]

Tech Stack
Core Technologies
Component	Technology	Version
Language	Java	21
Framework	Spring Boot	3.4.0
Build Tool	Gradle	8.10.2
ORM	Hibernate (JPA)	-
Database	MySQL	-
Message Queue	Apache Kafka	-
Cloud Storage	AWS S3	-
Key Dependencies
dependencies {
    // Spring Boot Starters
    implementation 'org.springframework.boot:spring-boot-starter-web'
    implementation 'org.springframework.boot:spring-boot-starter-data-jpa'
    implementation 'org.springframework.kafka:spring-kafka'
    
    // AWS SDK
    implementation 'software.amazon.awssdk:s3'
    
    // HTTP Clients
    implementation 'org.apache.httpcomponents.client5:httpclient5:5.4.1'
    
    // Utilities
    compileOnly 'org.projectlombok:lombok'
    implementation 'com.fasterxml.jackson.core:jackson-databind'
    
    // Database
    runtimeOnly 'com.mysql:mysql-connector-j'
}
Infrastructure Requirements
JDK: 21+
MySQL: 8.0+
Kafka: 2.8+
AWS Account: S3 bucket access
Polling and Health Check Mechanisms
Polling Strategy
Objective: Confirm asynchronous upload completion on provider side

Implementation: MirrorPollingScheduler

@Scheduled(fixedDelayString = "${mirror.polling.dispatch-delay-seconds:30}000")
public void pollPendingProviders() {
    List<MirrorProvider> pending = repository.findProvidersForPolling(
        Instant.now(), 
        PageRequest.of(0, batchSize)
    );
    
    pending.forEach(provider -> {
        ProviderPollResponse response = providerService.poll(provider);
        
        if (response.isComplete()) {
            provider.setStatus(ProviderStatus.ACTIVE);
            provider.setExternalFileId(response.getFileId());
        } else if (provider.getPollAttemptCount() >= maxAttempts) {
            provider.setStatus(ProviderStatus.FAILED);
        } else {
            provider.setNextPollAt(calculateNextPollTime());
        }
        
        repository.save(provider);
    });
}
Exponential Backoff Formula:

nextDelay = min(initialDelay * 2^attemptCount, maxDelay)
Configuration:

mirror:
  polling:
    initial-delay-seconds: 60
    max-delay-seconds: 1800
    max-attempts: 3
    dispatch-delay-seconds: 30
    batch-size: 50
Health Check System
Objective: Ensure long-term file availability and detect provider issues

Implementation: MirrorRecheckScheduler

@Scheduled(cron = "${mirror.recheck.cron:0 0 0,12 * * *}")
public void recheckActiveProviders() {
    Instant cutoff = Instant.now().minus(recheckPeriodDays, ChronoUnit.DAYS);
    
    List<MirrorProvider> providers = repository.findProvidersForRecheck(cutoff);
    
    providers.forEach(provider -> {
        try {
            FileInfoResponse info = providerService.getFileInfo(
                provider.getExternalFileId()
            );
            
            if (info.exists()) {
                provider.setLastCheckedAt(Instant.now());
            } else {
                provider.setStatus(ProviderStatus.FAILED);
                provider.setLastError("File not found on provider");
            }
        } catch (Exception e) {
            log.error("Recheck failed for provider {}", provider.getId(), e);
        }
        
        repository.save(provider);
    });
}
Recheck Configuration:

mirror:
  recheck:
    period-days: 30
    cron: "0 0 0,12 * * *"  # Twice daily
    batch-size: 100
Monitoring & Observability
Logging:

Structured JSON logs with MDC (Mapped Diagnostic Context)
Request tracing via TraceIdGenerator
Provider-specific log markers
Metrics (Ready for Prometheus integration):

Job success/failure rates
Provider-specific upload latencies
Polling attempt distributions
Recheck failure counts
Configuration
Application Properties (application.yaml)
server:
  port: 8082
  servlet:
    context-path: /api

spring:
  datasource:
    url: jdbc:mysql://localhost:3306/replicator_db
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
  
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      group-id: replicator-service
      auto-offset-reset: earliest
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: '*'

aws:
  s3:
    bucket-name: amzn-file-mirror-bucket
    region: ap-south-1

mirror:
  enabled-providers:
    - STREAM_TAPE
    - KRAKEN_FILE
  
  stream-tape:
    api-url: https://api.streamtape.com
    api-login: ${STREAMTAPE_API_LOGIN}
    api-key: ${STREAMTAPE_API_KEY}
  
  kafka:
    topics:
      file-upload: file_upload
      file-mirrored: file_mirrored
      file-mirror-check: file_mirror_check
  
  polling:
    initial-delay-seconds: 60
    max-delay-seconds: 1800
    max-attempts: 3
    dispatch-delay-seconds: 30
    batch-size: 50
  
  recheck:
    period-days: 30
    cron: "0 0 0,12 * * *"
    batch-size: 100
Environment Variables
Variable	Description	Required
DB_USERNAME	MySQL username	Yes
DB_PASSWORD	MySQL password	Yes
KAFKA_BOOTSTRAP_SERVERS	Kafka broker addresses	Yes
STREAMTAPE_API_LOGIN	StreamTape API login	Yes
STREAMTAPE_API_KEY	StreamTape API key	Yes
AWS_ACCESS_KEY_ID	AWS credentials	Yes
AWS_SECRET_ACCESS_KEY	AWS credentials	Yes
Getting Started
Prerequisites
JDK 21+
MySQL 8.0+
Kafka cluster
AWS account with S3 access
Build & Run
# Build the application
./gradlew build

# Run locally
./gradlew bootRun

# Run with custom profile
./gradlew bootRun --args='--spring.profiles.active=dev'

# Build Docker image
docker build -t replicator-service:latest .

# Run with Docker Compose
docker-compose up -d
Database Setup
CREATE DATABASE replicator_db;

-- Tables are auto-created via Hibernate DDL
-- For production, use Flyway/Liquibase migrations
API Reference
Kafka Topics
Topic	Type	Schema
file_upload	Consumed	FileUploadEvent
file_mirrored	Produced	FileMirrorEvent
file_mirror_check	Produced	MirrorCheckEvent
Distributed Systems Concepts
Asynchronous Processing
@Async Methods: Non-blocking job execution via Spring's task executor
Thread Pool: Configurable executor with proper queue sizing
CompletableFuture: Used for parallel provider operations
Idempotency
Unique Constraints: Prevents duplicate file records
Job Status Checks: Avoids re-processing completed jobs
Kafka Consumer Offsets: At-least-once delivery semantics
Fault Tolerance
Exponential Backoff: Prevents provider overload during outages
Circuit Breaker: (Planned) Resilience4j integration
Dead Letter Queue: (Planned) Failed event handling
Scalability
Horizontal Scaling: Stateless service design
Kafka Consumer Groups: Parallel message consumption
Batch Processing: Efficient polling via batch queries
Microservices Architecture
Service Boundaries
Replicator Service: File mirroring orchestration
Upload Service: (External) File ingestion and S3 upload
Notification Service: (Planned) User notifications on completion
Inter-Service Communication
Kafka: Async event-driven communication
REST: (Future) Synchronous queries for status checks
Data Ownership
Replicator: Owns replication job and provider status data
Upload Service: Owns source file metadata
Built with ❤️ for the Multi Mirror Project
