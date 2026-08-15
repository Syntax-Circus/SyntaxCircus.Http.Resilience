# SyntaxCircus.Http.Resilience

[![Build](https://github.com/Syntax-Circus/SyntaxCircus.Http.Resilience/actions/workflows/build.yml/badge.svg)](https://github.com/Syntax-Circus/SyntaxCircus.Http.Resilience/actions/workflows/build.yml)
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

JSON `GetAsync`/`GetWithETagAsync` (conditional GET with a per-URL ETag cache)/`PostAsync`/`PutAsync` (with `If-Match` from the cached ETag)/`DeleteAsync`, and centralized error handling: a non-success response is translated into a `ProblemDetailsException` (`StatusCode`, `Type`, `Title`, `Errors`) when the body is an RFC 7807 ProblemDetails payload. Bearer-token attachment is left to the caller — register a `DelegatingHandler` on the typed client via `AddHttpMessageHandler<T>()` rather than baking auth into the base class.

## AddResilientHttpClient

```csharp
builder.Services.AddResilientHttpClient(
    "widgets-api",
    client => client.BaseAddress = new Uri("https://widgets.example.com"),
    retryCount: 3)
    .AddTypedClient<WidgetApiClient>();
```

Wraps the named `HttpClient` in a Polly retry (exponential backoff + jitter) and circuit-breaker pipeline, retrying transient errors and 429/5xx. Pass `aiMode: true` for AI/LLM provider clients where a 429 means "back off on purpose" rather than "something's broken" — it's excluded from retry/circuit-breaking in that mode.

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

## Contributing

Issues and pull requests are welcome:
- Keep changes focused, with a clear description of the behavior change.
- Match the existing code style (see `.editorconfig`).
- Call out any breaking changes to the public API in your PR description.

## License

MIT — see [LICENSE.txt](LICENSE.txt).
