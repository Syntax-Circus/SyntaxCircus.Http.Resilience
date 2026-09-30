namespace SyntaxCircus.Http.Resilience.Tests;

public sealed class OutboxConsumerTests
{
    [Theory]
    [InlineData(HttpStatusCode.TooManyRequests)]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    public async Task OutboxPolicy_TransientPostResponse_IsReturnedWithoutNestedRetry(HttpStatusCode status)
    {
        var sends = 0;
        var pipeline = new HttpRequestResiliencePipeline("operations-webhook", new HttpRequestResilienceOptions
        {
            MaxAttempts = 1,
            TotalRequestTimeout = TimeSpan.FromSeconds(30)
        });
        using var response = await pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Post, "https://example.invalid/webhook")),
            (request, _, _) =>
            {
                request.Method.ShouldBe(HttpMethod.Post);
                Interlocked.Increment(ref sends);
                var reply = new HttpResponseMessage(status);
                reply.Headers.RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromMinutes(2));
                return Task.FromResult(reply);
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.NotReplayable,
            cancellationToken: TestContext.Current.CancellationToken);
        sends.ShouldBe(1);
        response.StatusCode.ShouldBe(status);
        response.Headers.RetryAfter!.Delta.ShouldBe(TimeSpan.FromMinutes(2));
    }
}
