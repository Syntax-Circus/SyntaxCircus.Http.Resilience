# Durable-outbox consumers

Use the existing HttpRequestResiliencePipeline when durable application state owns retries. Do not wrap it in AddResilientHttpClient or another retry handler: one outbox attempt must mean one actual HTTP send.

```csharp
var pipeline = new HttpRequestResiliencePipeline("operations-webhook",
    new HttpRequestResilienceOptions
    {
        MaxAttempts = 1,
        TotalRequestTimeout = TimeSpan.FromSeconds(30),
        TimeProvider = timeProvider,
    });

using var response = await pipeline.SendAsync(
    (_, _) => ValueTask.FromResult(CreateFreshNotificationRequest(notificationId)),
    (request, completion, ct) => httpClient.SendAsync(request, completion, ct),
    HttpCompletionOption.ResponseHeadersRead,
    HttpRequestReplaySafety.NotReplayable,
    responseObserver: ObserveBoundedResponseAsync,
    cancellationToken: cancellationToken);
```

The caller owns these example helpers: CreateFreshNotificationRequest constructs the approved POST payload and idempotency header; ObserveBoundedResponseAsync reads only a bounded response body under the supplied cancellation token if the endpoint requires one. The observer is inside the logical deadline. Reading response content after SendAsync returns is not covered by that deadline. Never forward an unbounded provider response into logs or durable state.

Configure the underlying HttpClient handler with redirects disabled. Keep the request URL, authentication and provider response content out of telemetry; pipeline names must be approved aliases, not URLs. Do not attach an automatic retry handler. A stable notification identifier does not make POST delivery exactly-once unless the receiver implements durable deduplication.

Persist HTTP status and a safe application-defined failure code. The outbox decides transient/permanent failures, next due time, provider Retry-After lower bounds, and uncertain delivery handling. Return 429 to that coordinator rather than sleeping inside an additional retry loop. Caller cancellation is distinct from a logical timeout; after ambiguous timeout/disconnection a receiver may already have accepted the request.

Circuit state is process-local. A daily CLI or timer-launched dispatcher must not assume a circuit survives process exit. Persisted scheduling and leases remain application responsibilities. This package does not own SQLite queues, notification payloads, Discord behavior, recipient authorization or PKI policy.

Read-only operations proven replayable may use MaxAttempts=3 under one 30-second total budget and HttpRequestReplaySafety.Replayable. Every attempt still constructs a fresh request; response/request ownership and disposal remain as documented in the README.
