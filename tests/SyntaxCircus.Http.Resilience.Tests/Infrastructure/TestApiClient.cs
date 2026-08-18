namespace SyntaxCircus.Http.Resilience.Tests.Infrastructure;

/// <summary>Exposes <see cref="ApiClientBase"/>'s protected members for direct testing.</summary>
internal sealed class TestApiClient(HttpClient httpClient) : ApiClientBase(httpClient)
{
    private readonly List<HttpResponseMessage> _observedResponses = [];

    public IReadOnlyList<HttpResponseMessage> ObservedResponses => _observedResponses;

    protected override Task OnResponseReceivedAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        _observedResponses.Add(response);
        return Task.CompletedTask;
    }

    public List<HttpRequestMessage> ObservedRequests { get; } = [];

    protected override Task OnRequestSendingAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        ObservedRequests.Add(request);
        MutateRequest?.Invoke(request);
        return Task.CompletedTask;
    }

    public Action<HttpRequestMessage>? MutateRequest { get; set; }

    public Task<T?> Get<T>(string requestUri, CancellationToken cancellationToken = default)
        => GetAsync<T>(requestUri, cancellationToken);

#pragma warning disable CA1068 // Mirrors ApiClientBase.GetWithETagAsync's intentional non-last cancellationToken.
    public Task<T?> GetWithETag<T>(string requestUri, CancellationToken cancellationToken = default, bool useConditionalRequest = true)
#pragma warning restore CA1068
        => GetWithETagAsync<T>(requestUri, cancellationToken, useConditionalRequest);

    public Task<TResponse?> Post<TRequest, TResponse>(string requestUri, TRequest body, CancellationToken cancellationToken = default)
        => PostAsync<TRequest, TResponse>(requestUri, body, cancellationToken);

    public Task Post<TRequest>(string requestUri, TRequest body, CancellationToken cancellationToken = default)
        => PostAsync(requestUri, body, cancellationToken);

    public Task Put<TRequest>(string requestUri, TRequest body, CancellationToken cancellationToken = default)
        => PutAsync(requestUri, body, cancellationToken);

    public Task<TResponse?> Put<TRequest, TResponse>(string requestUri, TRequest body, CancellationToken cancellationToken = default)
        => PutAsync<TRequest, TResponse>(requestUri, body, cancellationToken);

    public Task Delete(string requestUri, CancellationToken cancellationToken = default)
        => DeleteAsync(requestUri, cancellationToken);

    public Task<HttpResponseMessage> Send(HttpRequestMessage request, CancellationToken cancellationToken = default)
        => SendAsync(request, cancellationToken);

    public static Task<T?> ReadJson<T>(HttpResponseMessage response, CancellationToken cancellationToken = default)
        => ReadJsonAsync<T>(response, cancellationToken);
}
