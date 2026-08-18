using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace SyntaxCircus.Http.Resilience;

/// <summary>
/// Base class for typed HTTP API clients: JSON GET/POST/PUT/DELETE helpers, per-URL ETag caching
/// with conditional GET / <c>If-Match</c>, and centralized ProblemDetails-to-exception translation.
/// Bearer-token attachment is left to the caller — e.g. a <c>DelegatingHandler</c> registered on
/// the typed client via <c>AddHttpMessageHandler</c> — this class only shapes requests/responses.
/// Override <see cref="OnResponseReceivedAsync"/> to observe response headers (e.g. a sliding
/// session-expiry header) on every call without re-implementing the verb helpers.
/// </summary>
public abstract class ApiClientBase(HttpClient httpClient)
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly Dictionary<string, string> _etagCache = new(StringComparer.Ordinal);

    protected HttpClient HttpClient { get; } = httpClient;

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
        using var response = await HttpClient.GetAsync(requestUri, cancellationToken).ConfigureAwait(false);
        await OnResponseReceivedAsync(response, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        return await response.Content.ReadFromJsonAsync<T>(SerializerOptions, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>GETs with a conditional request when a cached ETag exists for this URL; returns <c>default</c> on a 304.</summary>
    protected async Task<T?> GetWithETagAsync<T>(string requestUri, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, requestUri);
        if (_etagCache.TryGetValue(requestUri, out var etag))
        {
            request.Headers.TryAddWithoutValidation("If-None-Match", etag);
        }

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
        using var response = await HttpClient.PostAsJsonAsync(requestUri, body, SerializerOptions, cancellationToken).ConfigureAwait(false);
        await OnResponseReceivedAsync(response, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        return await response.Content.ReadFromJsonAsync<TResponse>(SerializerOptions, cancellationToken).ConfigureAwait(false);
    }

    protected async Task PostAsync<TRequest>(string requestUri, TRequest body, CancellationToken cancellationToken = default)
    {
        using var response = await HttpClient.PostAsJsonAsync(requestUri, body, SerializerOptions, cancellationToken).ConfigureAwait(false);
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

        using var response = await HttpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        await OnResponseReceivedAsync(response, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        CacheETag(requestUri, response);
    }

    protected async Task DeleteAsync(string requestUri, CancellationToken cancellationToken = default)
    {
        using var response = await HttpClient.DeleteAsync(requestUri, cancellationToken).ConfigureAwait(false);
        await OnResponseReceivedAsync(response, cancellationToken).ConfigureAwait(false);
        await EnsureSuccessAsync(response, cancellationToken).ConfigureAwait(false);
        _etagCache.Remove(requestUri);
    }

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
