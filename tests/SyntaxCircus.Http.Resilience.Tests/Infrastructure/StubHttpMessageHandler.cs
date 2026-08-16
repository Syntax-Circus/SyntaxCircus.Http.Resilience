namespace SyntaxCircus.Http.Resilience.Tests.Infrastructure;

/// <summary>
/// Fake <see cref="HttpMessageHandler"/> that hands requests to a caller-supplied responder and
/// snapshots the outgoing request (method, URI, headers, body) before the real
/// <see cref="HttpRequestMessage"/> gets disposed by the client under test.
/// </summary>
internal sealed class StubHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
{
    public CapturedRequest? LastRequest { get; private set; }

    public int CallCount { get; private set; }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        CallCount++;

        var body = request.Content is null
            ? null
            : await request.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);

        LastRequest = new CapturedRequest(
            request.Method,
            request.RequestUri,
            request.Headers.ToDictionary(h => h.Key, h => h.Value, StringComparer.OrdinalIgnoreCase),
            body);

        return responder(request);
    }
}

internal sealed record CapturedRequest(
    HttpMethod Method,
    Uri? RequestUri,
    IReadOnlyDictionary<string, IEnumerable<string>> Headers,
    string? Body)
{
    public string? HeaderValue(string name) => Headers.TryGetValue(name, out var values) ? values.FirstOrDefault() : null;
}
