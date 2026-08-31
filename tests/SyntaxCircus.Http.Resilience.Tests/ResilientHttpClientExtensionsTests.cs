namespace SyntaxCircus.Http.Resilience.Tests;

public class ResilientHttpClientExtensionsTests
{
    private static (HttpClient Client, StubHttpMessageHandler Handler) BuildClient(
        Func<HttpRequestMessage, HttpResponseMessage> responder,
        int retryCount = 2,
        bool aiMode = false)
    {
        var handler = new StubHttpMessageHandler(responder);
        var services = new ServiceCollection();
        services.AddResilientHttpClient("test-client", retryCount: retryCount, aiMode: aiMode)
            .ConfigurePrimaryHttpMessageHandler(() => handler);

        var provider = services.BuildServiceProvider();
        var httpClientFactory = provider.GetRequiredService<IHttpClientFactory>();
        return (httpClientFactory.CreateClient("test-client"), handler);
    }

    [Fact]
    public void AddResilientHttpClient_NullServices_ThrowsArgumentNullException()
    {
        Should.Throw<ArgumentNullException>(() =>
            ResilientHttpClientExtensions.AddResilientHttpClient(null!, "name"));
    }

    [Fact]
    public void AddResilientHttpClient_NullName_ThrowsArgumentException()
    {
        var services = new ServiceCollection();

        Should.Throw<ArgumentException>(() => services.AddResilientHttpClient(null!));
    }

    [Fact]
    public void AddResilientHttpClient_EmptyName_ThrowsArgumentException()
    {
        var services = new ServiceCollection();

        Should.Throw<ArgumentException>(() => services.AddResilientHttpClient("   "));
    }

    [Fact]
    public async Task SendAsync_PersistentServerError_RetriesUpToRetryCountThenReturnsFailure()
    {
        var (client, handler) = BuildClient(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError), retryCount: 2);

        var response = await client.GetAsync(new Uri("https://example.test/things"), TestContext.Current.CancellationToken);

        response.StatusCode.ShouldBe(HttpStatusCode.InternalServerError);
        handler.CallCount.ShouldBe(3);
    }

    [Fact]
    public async Task SendAsync_TransientFailureThenSuccess_RetriesUntilSuccessful()
    {
        var attempt = 0;
        var (client, handler) = BuildClient(_ =>
        {
            attempt++;
            return attempt == 1
                ? new HttpResponseMessage(HttpStatusCode.InternalServerError)
                : new HttpResponseMessage(HttpStatusCode.OK);
        });

        var response = await client.GetAsync(new Uri("https://example.test/things"), TestContext.Current.CancellationToken);

        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        handler.CallCount.ShouldBe(2);
    }

    [Fact]
    public async Task SendAsync_SuccessResponse_DoesNotRetry()
    {
        var (client, handler) = BuildClient(_ => new HttpResponseMessage(HttpStatusCode.OK));

        await client.GetAsync(new Uri("https://example.test/things"), TestContext.Current.CancellationToken);

        handler.CallCount.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_ClientErrorNotRetried()
    {
        var (client, handler) = BuildClient(_ => new HttpResponseMessage(HttpStatusCode.BadRequest));

        await client.GetAsync(new Uri("https://example.test/things"), TestContext.Current.CancellationToken);

        handler.CallCount.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_RequestTimeoutIsRetried()
    {
        var (client, handler) = BuildClient(_ => new HttpResponseMessage(HttpStatusCode.RequestTimeout), retryCount: 1);

        await client.GetAsync(new Uri("https://example.test/things"), TestContext.Current.CancellationToken);

        handler.CallCount.ShouldBe(2);
    }

    [Fact]
    public async Task SendAsync_UndocumentedServerErrorIsNotRetried()
    {
        var (client, handler) = BuildClient(_ => new HttpResponseMessage(HttpStatusCode.NotImplemented), retryCount: 2);

        await client.GetAsync(new Uri("https://example.test/things"), TestContext.Current.CancellationToken);

        handler.CallCount.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_AiModeFalse_TooManyRequestsIsRetried()
    {
        var (client, handler) = BuildClient(_ => new HttpResponseMessage(HttpStatusCode.TooManyRequests), retryCount: 2, aiMode: false);

        await client.GetAsync(new Uri("https://example.test/things"), TestContext.Current.CancellationToken);

        handler.CallCount.ShouldBe(3);
    }

    [Fact]
    public async Task SendAsync_AiModeTrue_TooManyRequestsIsNotRetried()
    {
        var (client, handler) = BuildClient(_ => new HttpResponseMessage(HttpStatusCode.TooManyRequests), retryCount: 2, aiMode: true);

        var response = await client.GetAsync(new Uri("https://example.test/things"), TestContext.Current.CancellationToken);

        response.StatusCode.ShouldBe(HttpStatusCode.TooManyRequests);
        handler.CallCount.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_RepeatedFailures_OpensCircuitAndFastFailsSubsequentCalls()
    {
        var (client, _) = BuildClient(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError), retryCount: 1);

        // Drive enough failing calls to exceed the breaker's MinimumThroughput; a BrokenCircuitException
        // partway through just means the circuit tripped open earlier than this call, which is fine here.
        for (var i = 0; i < 5; i++)
        {
            try
            {
                await client.GetAsync(new Uri("https://example.test/things"), TestContext.Current.CancellationToken);
            }
            catch (BrokenCircuitException)
            {
            }
        }

        await Should.ThrowAsync<BrokenCircuitException>(() =>
            client.GetAsync(new Uri("https://example.test/things"), TestContext.Current.CancellationToken));
    }
}
