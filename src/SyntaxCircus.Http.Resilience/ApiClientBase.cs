using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace SyntaxCircus.Http.Resilience;

/// <summary>
/// Base class for typed HTTP API clients: JSON GET/POST/PUT/DELETE helpers, per-URL ETag caching
/// with conditional GET / <c>If-Match</c>, and centralized ProblemDetails-to-exception translation.
/// Bearer-token attachment is left to the caller — e.g. a <c>DelegatingHandler</c> registered on
/// the typed client via <c>AddHttpMessageHandler</c> for app-wide/singleton-safe auth (such as
/// <see cref="CachedTokenProvider"/>-backed client-credentials tokens), or by overriding
/// <see cref="OnRequestSendingAsync"/> in a derived class when auth is scoped to something only
/// available in the same DI scope as the typed client itself (e.g. a per-user/per-session token in
/// a web app) — <c>AddHttpMessageHandler</c>-registered handlers are resolved from a pooled,
/// periodically-rotated handler scope, not the caller's ambient scope, so they should only depend on
/// singleton-safe services. Override <see cref="OnResponseReceivedAsync"/> to observe response
/// headers (e.g. a sliding session-expiry header) on every call without re-implementing the verb
/// helpers.
/// </summary>
public abstract class ApiClientBase(HttpClient httpClient)
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly Dictionary<string, string> _etagCache = new(StringComparer.Ordinal);

    protected HttpClient HttpClient { get; } = httpClient;

    /// <summary>
    /// Called immediately before every request this client sends, after the request has been fully
    /// built (URL, conditional/ETag headers, JSON body) but before it's dispatched. No-op by default;
    /// override to attach cross-cutting request headers (e.g. bearer-token auth, a correlation ID)
    /// from a dependency that must be resolved in the same DI scope as the typed client itself —
    /// unlike a <c>DelegatingHandler</c> added via <c>AddHttpMessageHandler</c>, which is created in a
    /// separate, pooled scope and so shouldn't depend on scoped/per-request services.
    /// </summary>
    protected virtual Task OnRequestSendingAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        => Task.CompletedTask;

    /// <summary>
    /// Called after every response this client receives — success or error — before
    /// success/error handling (<see cref="EnsureSuccessAsync"/>) runs. No-op by default;
    /// override to observe response headers (e.g. a sliding session-expiry header) without
    /// re-implementing the verb helpers or overriding <c>SendAsync</c> on the underlying
    /// <see cref="HttpClient"/>.
    /// </summary>
    protected virtual Task OnResponseReceivedAsync(HttpResponseMessage response, CancellationToken cancellationToken)
        => Task.CompletedTask;

    protected async Task<T?> GetAsync<T>(string requestUri, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, requestUri);
        await OnRequestSendingAsync(request, cancellationToken).ConfigureAwait(false);
        using var response = await HttpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        await OnResponseReceivedAsync(response, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        return await response.Content.ReadFromJsonAsync<T>(SerializerOptions, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// GETs with a conditional request when a cached ETag exists for this URL; returns <c>default</c> on a 304.
    /// Pass <paramref name="useConditionalRequest"/> as <see langword="false"/> to always issue a plain GET (no
    /// <c>If-None-Match</c>, never a 304) while still caching the response's ETag for a later
    /// <see cref="PutAsync{TRequest}"/>/<see cref="DeleteAsync"/> — useful for long-lived typed-client instances
    /// (e.g. one per Blazor Server circuit) where a caller always wants a fresh body from a "load for edit"
    /// call, even if the same URL was already read earlier in the same client's lifetime.
    /// </summary>
#pragma warning disable CA1068 // Non-breaking parameter addition: cancellationToken must stay in its original (non-last) position to avoid a breaking change to existing callers.
    protected async Task<T?> GetWithETagAsync<T>(string requestUri, CancellationToken cancellationToken = default, bool useConditionalRequest = true)
#pragma warning restore CA1068
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, requestUri);
        if (useConditionalRequest && _etagCache.TryGetValue(requestUri, out var etag))
        {
            request.Headers.TryAddWithoutValidation("If-None-Match", etag);
        }

        await OnRequestSendingAsync(request, cancellationToken).ConfigureAwait(false);
        using var response = await HttpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        await OnResponseReceivedAsync(response, cancellationToken).ConfigureAwait(false);
        if (response.StatusCode == HttpStatusCode.NotModified)
        {
            return default;
        }

        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        CacheETag(requestUri, response);
        return await response.Content.ReadFromJsonAsync<T>(SerializerOptions, cancellationToken).ConfigureAwait(false);
    }

    protected async Task<TResponse?> PostAsync<TRequest, TResponse>(string requestUri, TRequest body, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, requestUri)
        {
            Content = JsonContent.Create(body, options: SerializerOptions),
        };

        await OnRequestSendingAsync(request, cancellationToken).ConfigureAwait(false);
        using var response = await HttpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        await OnResponseReceivedAsync(response, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        return await response.Content.ReadFromJsonAsync<TResponse>(SerializerOptions, cancellationToken).ConfigureAwait(false);
    }

    protected async Task PostAsync<TRequest>(string requestUri, TRequest body, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, requestUri)
        {
            Content = JsonContent.Create(body, options: SerializerOptions),
        };

        await OnRequestSendingAsync(request, cancellationToken).ConfigureAwait(false);
        using var response = await HttpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        await OnResponseReceivedAsync(response, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>PUTs, attaching <c>If-Match</c> from a cached ETag for this URL when one is known.</summary>
    protected async Task PutAsync<TRequest>(string requestUri, TRequest body, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Put, requestUri)
        {
            Content = JsonContent.Create(body, options: SerializerOptions),
        };

        if (_etagCache.TryGetValue(requestUri, out var etag))
        {
            request.Headers.TryAddWithoutValidation("If-Match", etag);
        }

        await OnRequestSendingAsync(request, cancellationToken).ConfigureAwait(false);
        using var response = await HttpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        await OnResponseReceivedAsync(response, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        CacheETag(requestUri, response);
    }

    protected async Task DeleteAsync(string requestUri, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Delete, requestUri);
        await OnRequestSendingAsync(request, cancellationToken).ConfigureAwait(false);
        using var response = await HttpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        await OnResponseReceivedAsync(response, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        _etagCache.Remove(requestUri);
    }

    /// <summary>
    /// Sends an arbitrary <see cref="HttpRequestMessage"/> (e.g. multipart form content, or a binary
    /// download) through the same pipeline as the JSON verb helpers: <see cref="OnRequestSendingAsync"/>,
    /// <see cref="OnResponseReceivedAsync"/>, ProblemDetails translation on non-success, and ETag caching
    /// for the request URI on success. Use this when the JSON-shaped
    /// <c>GetAsync</c>/<c>PostAsync</c>/<c>PutAsync</c>/<c>DeleteAsync</c> helpers don't fit (non-JSON
    /// request bodies, custom headers, streamed responses). The caller owns disposing the returned
    /// response and reading its content (see <see cref="ReadJsonAsync{T}"/> for a JSON body).
    /// </summary>
    protected async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken = default)
    {
        // HttpClient.SendAsync mutates RequestUri in place (resolving it against BaseAddress), so capture the
        // caller's original URI string before sending — matching the exact string other verb helpers cache under.
        var requestUri = request.RequestUri?.OriginalString;
        await OnRequestSendingAsync(request, cancellationToken).ConfigureAwait(false);
        var response = await HttpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        await OnResponseReceivedAsync(response, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        if (requestUri is not null)
        {
            CacheETag(requestUri, response);
        }

        return response;
    }

    /// <summary>Reads a JSON response body with this class's <see cref="SerializerOptions"/>. Pairs with <see cref="SendAsync"/>.</summary>
    protected static Task<T?> ReadJsonAsync<T>(HttpResponseMessage response, CancellationToken cancellationToken = default) =>
        response.Content.ReadFromJsonAsync<T>(SerializerOptions, cancellationToken);

    private void CacheETag(string requestUri, HttpResponseMessage response)
    {
        var etag = response.Headers.ETag?.Tag;
        if (!string.IsNullOrWhiteSpace(etag))
        {
            _etagCache[requestUri] = etag;
        }
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }

        ProblemDetailsPayload? problem = null;
        if (response.Content.Headers.ContentType?.MediaType is "application/problem+json" or "application/json")
        {
            try
            {
                problem = await response.Content.ReadFromJsonAsync<ProblemDetailsPayload>(SerializerOptions, cancellationToken).ConfigureAwait(false);
            }
            catch (JsonException)
            {
                // No parseable body — fall through to a generic exception below.
            }
        }

        throw new ProblemDetailsException(
            (int)response.StatusCode,
            problem?.Type,
            problem?.Title,
            problem?.Detail,
            problem?.Errors);
    }

    private sealed class ProblemDetailsPayload
    {
        public string? Type { get; init; }

        public string? Title { get; init; }

        public string? Detail { get; init; }

        public Dictionary<string, string[]>? Errors { get; init; }
    }
}
