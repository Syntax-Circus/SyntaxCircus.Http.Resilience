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

    public Task<T?> Get<T>(string requestUri, CancellationToken cancellationToken = default)
        => GetAsync<T>(requestUri, cancellationToken);

    public Task<T?> GetWithETag<T>(string requestUri, CancellationToken cancellationToken = default)
        => GetWithETagAsync<T>(requestUri, cancellationToken);

    public Task<TResponse?> Post<TRequest, TResponse>(string requestUri, TRequest body, CancellationToken cancellationToken = default)
        => PostAsync<TRequest, TResponse>(requestUri, body, cancellationToken);

    public Task Post<TRequest>(string requestUri, TRequest body, CancellationToken cancellationToken = default)
        => PostAsync(requestUri, body, cancellationToken);

    public Task Put<TRequest>(string requestUri, TRequest body, CancellationToken cancellationToken = default)
        => PutAsync(requestUri, body, cancellationToken);

    public Task Delete(string requestUri, CancellationToken cancellationToken = default)
        => DeleteAsync(requestUri, cancellationToken);
}
