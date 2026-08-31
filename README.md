# SyntaxCircus.Http.Resilience

[![Build](https://github.com/Syntax-Circus/SyntaxCircus.Http.Resilience/actions/workflows/build.yml/badge.svg)](https://github.com/Syntax-Circus/SyntaxCircus.Http.Resilience/actions/workflows/build.yml)
[![NuGet](https://img.shields.io/nuget/v/SyntaxCircus.Http.Resilience.svg)](https://www.nuget.org/packages/SyntaxCircus.Http.Resilience)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE.txt)

A typed API client base, a Polly-based resilient `HttpClient` registration helper, and a generic cached-token provider — the pieces that keep getting rewritten every time a product calls another API.

> **No support guaranteed.** Published as-is and maintained on a best-effort basis. Issues and PRs are welcome, but there's no SLA — fork it or vendor what you need if that's not enough.

## ApiClientBase

```csharp
public sealed class WidgetApiClient(HttpClient httpClient) : ApiClientBase(httpClient)
{
    public Task<Widget?> GetWidgetAsync(string id, CancellationToken ct) => GetAsync<Widget>($"widgets/{id}", ct);

    public Task CreateWidgetAsync(Widget widget, CancellationToken ct) => PostAsync("widgets", widget, ct);
}
```

JSON `GetAsync`/`GetWithETagAsync` (conditional GET with a per-URL ETag cache)/`PostAsync`/`PutAsync`/`DeleteAsync` (`PutAsync`/`DeleteAsync` both attach `If-Match` from the cached ETag when one is known), and centralized error handling: a non-success response is translated into a `ProblemDetailsException` (`StatusCode`, `Type`, `Title`, `Errors`) when the body is an RFC 7807 ProblemDetails payload. Bearer-token attachment is left to the caller: for app-wide/singleton-safe auth (e.g. a `CachedTokenProvider`-backed client-credentials token), register a `DelegatingHandler` on the typed client via `AddHttpMessageHandler<T>()`; for auth scoped to something only available in the same DI scope as the typed client itself (e.g. a per-user/per-session token in a web app), override `OnRequestSendingAsync` in a derived class instead — `AddHttpMessageHandler`-registered handlers are resolved from a pooled, periodically-rotated handler scope, not the caller's ambient DI scope, so they must only depend on singleton-safe services.

```csharp
public sealed class AuthenticatedWidgetApiClient(HttpClient httpClient, IMyScopedTokenAccessor tokenAccessor)
    : ApiClientBase(httpClient)
{
    protected override async Task OnRequestSendingAsync(HttpRequestMessage request, CancellationToken ct)
    {
        var token = await tokenAccessor.GetTokenAsync(ct);
        if (!string.IsNullOrWhiteSpace(token))
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        }
    }
}
```

`GetWithETagAsync` takes an optional `useConditionalRequest` parameter (default `true`, preserving the conditional-GET/304 behavior above). Pass `false` for a "load for edit" call that should always return a fresh body — no `If-None-Match` is sent and a 304 can never happen, but the response's ETag is still cached for a subsequent `PutAsync`/`DeleteAsync` on the same URL. Useful for long-lived typed-client instances (e.g. one per web-app session/circuit) where a caller reads the same URL more than once and always wants the current value, not a cached-away 304.

`PutAsync<TRequest>(url, body, ct)` returns no body (just success/failure) — use `PutAsync<TRequest, TResponse>(url, body, ct)` instead when the API returns the updated resource from a successful update (e.g. server-computed fields, an updated timestamp). Both overloads attach `If-Match` from the cached ETag and re-cache the response's ETag on success.

Need to send something the JSON verb helpers don't fit — multipart form content, a binary download, custom headers? Use `SendAsync(HttpRequestMessage, ct)` / `ReadJsonAsync<T>(HttpResponseMessage, ct)`, protected members that run the same `OnRequestSendingAsync`/`OnResponseReceivedAsync` hooks, ProblemDetails translation, and ETag caching as the verb helpers, while leaving you in control of the request/response shape:

```csharp
public async Task<Widget> UploadAsync(Stream file, CancellationToken ct)
{
    using var request = new HttpRequestMessage(HttpMethod.Post, "widgets/upload") { Content = new StreamContent(file) };
    using var response = await SendAsync(request, ct);
    return (await ReadJsonAsync<Widget>(response, ct))!;
}
```

## AddResilientHttpClient

```csharp
builder.Services.AddResilientHttpClient(
    "widgets-api",
    client => client.BaseAddress = new Uri("https://widgets.example.com"),
    retryCount: 3)
    .AddTypedClient<WidgetApiClient>();
```

Wraps the named `HttpClient` in a Polly retry (exponential backoff + jitter) and circuit-breaker pipeline, retrying transport failures, non-caller timeouts, and exactly HTTP 408, 429, 500, 502, 503, and 504. Pass `aiMode: true` for AI/LLM provider clients where a 429 means "back off on purpose" rather than "something's broken" — only 429 is excluded from retry/circuit-breaking in that mode.

## HttpRequestResiliencePipeline

`HttpRequestResiliencePipeline` is the request-execution API for code that needs explicit retry ownership. Construct it directly when the policy is local to a consumer:

```csharp
var pipeline = new HttpRequestResiliencePipeline(
    "widgets",
    new HttpRequestResilienceOptions
    {
        MaxAttempts = 3,
        TotalRequestTimeout = TimeSpan.FromSeconds(30),
        BackoffBaseDelay = TimeSpan.FromMilliseconds(100),
        MaximumDelay = TimeSpan.FromSeconds(5),
        RetryableStatusCodes = new HashSet<HttpStatusCode>
        {
            HttpStatusCode.RequestTimeout,
            HttpStatusCode.TooManyRequests,
            HttpStatusCode.InternalServerError,
            HttpStatusCode.BadGateway,
            HttpStatusCode.ServiceUnavailable,
            HttpStatusCode.GatewayTimeout,
        },
        RetryableExceptionCategories = new HashSet<HttpResilienceFailureCategory>
        {
            HttpResilienceFailureCategory.Transport,
            HttpResilienceFailureCategory.Timeout,
        },
        OnTimeout = (telemetry, cancellationToken) =>
        {
            logger.LogWarning(
                "Pipeline {PipelineName} exhausted its {Timeout} budget ({FailureCategory})",
                telemetry.PipelineName,
                telemetry.Timeout,
                telemetry.FailureCategory);
            return ValueTask.CompletedTask;
        },
    });

using var response = await pipeline.SendAsync(
    (attempt, _) => ValueTask.FromResult(
        new HttpRequestMessage(HttpMethod.Get, $"https://widgets.example.com/{attempt}")),
    (request, completionOption, cancellationToken) => httpClient.SendAsync(request, completionOption, cancellationToken),
    HttpCompletionOption.ResponseHeadersRead,
    HttpRequestReplaySafety.Replayable,
    cancellationToken: cancellationToken);
```

For shared named policies, register a keyed singleton and resolve it with the standard DI extension:

```csharp
services.AddHttpRequestResiliencePipeline(
    "widgets",
    new HttpRequestResilienceOptions
    {
        MaxAttempts = 3,
        TotalRequestTimeout = TimeSpan.FromSeconds(30),
        BackoffBaseDelay = TimeSpan.FromMilliseconds(100),
        MaximumDelay = TimeSpan.FromSeconds(5),
    });

var pipeline = serviceProvider.GetRequiredKeyedService<HttpRequestResiliencePipeline>("widgets");
using var response = await pipeline.SendAsync(
    (attempt, _) => ValueTask.FromResult(
        new HttpRequestMessage(HttpMethod.Get, $"https://widgets.example.com/{attempt}")),
    (request, completionOption, cancellationToken) => httpClient.SendAsync(request, completionOption, cancellationToken),
    HttpCompletionOption.ResponseHeadersRead,
    HttpRequestReplaySafety.Replayable,
    cancellationToken: cancellationToken);
```

Every attempt needs a **fresh request factory**: the pipeline disposes each request after its send, so never return a reused `HttpRequestMessage`. Mark an operation `Replayable` only when repeating it is safe; `NotReplayable` sends at most once. The returned final response belongs to the caller and must be disposed. A caller cancellation remains an `OperationCanceledException`; exhausting `TotalRequestTimeout` throws `HttpRequestTimeoutException`; an open circuit throws `HttpCircuitOpenException`.

`TotalRequestTimeout` is one hard monotonic deadline across request construction, sending, response observation, attempts, and delays. Every positive finite duration is accepted; `TimeSpan.MaxValue` means no logical deadline. If a factory, sender, or observer ignores cancellation, the caller still receives cancellation or timeout promptly. The pipeline retains its request/response ownership until that late work completes, observes late exceptions, runs the observer once for any late response, and disposes the late owned state. After a late sender completes, its completion thread only queues the observer work; a separate scheduling boundary precedes every part of the observer invocation, including its synchronous prefix, so observer code cannot re-enter that completion thread or delay terminal cancellation/timeout. Scheduled sender and observer delegates receive stable request/response snapshots that are unaffected when the pipeline transfers and clears its outer ownership locals.

`RetryableStatusCodes` defaults to 408, 429, 500, 502, 503, and 504. `RetryableExceptionCategories` defaults to `Transport` and `Timeout`; only those two exception categories are valid configuration values. Retry and circuit classification share these sets. The pipeline snapshots every option during construction, so later mutation cannot change a live policy. Caller cancellation and observer failure do not contribute circuit throughput or change circuit state; a canceled or observer-failed half-open probe releases the probe slot for the next real outcome.

Caller cancellation and logical circuit completion have one atomic terminal order. Cancellation observed by the breaker while completion is still pending—including cancellation raised during the injected completion-timestamp read—wins with the exact caller token and records no throughput or completion transition. Once completion commits under the breaker lock, it is terminal: cancellation first initiated afterward by circuit telemetry cannot retroactively replace that committed response, exception, or timeout. Circuit callbacks always run outside the breaker lock, so no transition is exposed and then rolled back.

`OnRetry`, `OnTimeout`, and `OnCircuitStateChanged` receive bounded retry, logical-budget timeout, and circuit telemetry. A timeout event contains only the pipeline name, `Timeout` failure category, and configured total budget. Callback failures are non-fatal and never replace success, timeout, caller cancellation, or another request outcome. Keyed registration validates and snapshots the supplied `HttpRequestResilienceOptions` as it creates the singleton. It intentionally adds neither a `DelegatingHandler` nor an `HttpClient`: migrate selected manual request paths to this API while retaining `AddResilientHttpClient` for existing `HttpClientFactory` registrations.

## CachedTokenProvider

```csharp
var tokenProvider = new CachedTokenProvider(async ct =>
{
    var token = await FetchClientCredentialsTokenAsync(ct);
    return new CachedToken(token.AccessToken, DateTimeOffset.UtcNow.AddSeconds(token.ExpiresIn));
});

var accessToken = await tokenProvider.GetTokenAsync();
```

A semaphore-guarded token cache, refreshed under lock once it's within `expirySkew` (default 60s) of expiry. The acquisition delegate is entirely up to you — client-credentials grant, a custom token endpoint, whatever your worker-to-API auth needs.

## Release process

Pull requests and ordinary branch pushes only build and test the repository. They never pack, publish, or tag a package.

An authorized maintainer starts the `Build` workflow manually with the exact package version and full commit SHA from `main`. The workflow builds and tests that source, packs the candidate once, and records its source, raw SHA-256, NuGet content hash, size, and checksums in the uploaded candidate artifact. Publication waits for approval in the protected `release` environment and reuses those exact uploaded bytes. The protected job rejects an existing version, publishes through NuGet trusted publishing, verifies the repository signature and preserved content hash, and creates the version tag only after the public package passes verification.

`publish.ps1` is a local pack-and-test helper only. It cannot publish packages or create tags.

## Contributing

Issues and pull requests are welcome:
- Keep changes focused, with a clear description of the behavior change.
- Match the existing code style (see `.editorconfig`).
- Call out any breaking changes to the public API in your PR description.

## License

MIT — see [LICENSE.txt](LICENSE.txt).
