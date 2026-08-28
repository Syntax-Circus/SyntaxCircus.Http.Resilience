namespace SyntaxCircus.Http.Resilience.Tests;

public class HttpRequestResilienceServiceCollectionExtensionsTests
{
    [Fact]
    public async Task AddHttpRequestResiliencePipeline_RegistersIsolatedKeyedSingletonsWithDirectPipelineParity()
    {
        var firstOptions = CreateOptions(maxAttempts: 2, backoffBaseDelay: TimeSpan.FromMilliseconds(1));
        var secondOptions = CreateOptions(maxAttempts: 3, backoffBaseDelay: TimeSpan.FromMilliseconds(2));
        var services = new ServiceCollection();

        services.AddHttpRequestResiliencePipeline("first", firstOptions);
        services.AddHttpRequestResiliencePipeline("second", secondOptions);

        using var serviceProvider = services.BuildServiceProvider();
        var first = serviceProvider.GetRequiredKeyedService<HttpRequestResiliencePipeline>("first");
        var sameFirst = serviceProvider.GetRequiredKeyedService<HttpRequestResiliencePipeline>("first");
        var second = serviceProvider.GetRequiredKeyedService<HttpRequestResiliencePipeline>("second");
        var sameSecond = serviceProvider.GetRequiredKeyedService<HttpRequestResiliencePipeline>("second");
        var direct = new HttpRequestResiliencePipeline("first", firstOptions);

        first.ShouldBeSameAs(sameFirst);
        second.ShouldBeSameAs(sameSecond);
        first.ShouldNotBeSameAs(second);

        using var directResponse = await SendServiceUnavailableThenOkAsync(direct);
        using var keyedResponse = await SendServiceUnavailableThenOkAsync(first);

        directResponse.StatusCode.ShouldBe(HttpStatusCode.OK);
        keyedResponse.StatusCode.ShouldBe(HttpStatusCode.OK);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void AddHttpRequestResiliencePipeline_RejectsBlankName(string name)
    {
        var services = new ServiceCollection();

        Should.Throw<ArgumentException>(() => services.AddHttpRequestResiliencePipeline(name, CreateOptions()));
    }

    [Fact]
    public void AddHttpRequestResiliencePipeline_RejectsNullOptions()
    {
        var services = new ServiceCollection();

        Should.Throw<ArgumentNullException>(() => services.AddHttpRequestResiliencePipeline("test", null!));
    }

    [Fact]
    public void AddHttpRequestResiliencePipeline_DoesNotRegisterHttpClientOrDelegatingHandler()
    {
        var services = new ServiceCollection();

        services.AddHttpRequestResiliencePipeline("test", CreateOptions());

        using var serviceProvider = services.BuildServiceProvider();
        serviceProvider.GetService<HttpClient>().ShouldBeNull();
        serviceProvider.GetService<DelegatingHandler>().ShouldBeNull();
    }

    private static HttpRequestResilienceOptions CreateOptions(int maxAttempts = 2, TimeSpan? backoffBaseDelay = null)
        => new()
        {
            MaxAttempts = maxAttempts,
            TotalRequestTimeout = TimeSpan.FromSeconds(5),
            BackoffBaseDelay = backoffBaseDelay ?? TimeSpan.FromMilliseconds(1),
            MaximumDelay = TimeSpan.FromMilliseconds(10),
            CircuitFailureRatio = 0.5,
            CircuitMinimumThroughput = 2,
            CircuitSamplingDuration = TimeSpan.FromSeconds(5),
            CircuitBreakDuration = TimeSpan.FromSeconds(5),
            JitterProvider = () => 0,
        };

    private static async Task<HttpResponseMessage> SendServiceUnavailableThenOkAsync(HttpRequestResiliencePipeline pipeline)
    {
        var attempts = 0;

        var response = await pipeline.SendAsync(
            (attempt, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, $"https://example.test/{attempt}")),
            (_, _, _) => Task.FromResult(++attempts == 1
                ? new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                : new HttpResponseMessage(HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        attempts.ShouldBe(2);
        return response;
    }
}
