using System.Net.Http.Headers;
using System.Text;

namespace SyntaxCircus.Http.Resilience.Tests;

public class ApiClientBaseTests
{
    private sealed record TestPayload(string Name, int Value);

    private static (TestApiClient Client, StubHttpMessageHandler Handler) CreateClient(Func<HttpRequestMessage, HttpResponseMessage> responder)
    {
        var handler = new StubHttpMessageHandler(responder);
        var httpClient = new HttpClient(handler) { BaseAddress = new Uri("https://example.test/") };
        return (new TestApiClient(httpClient), handler);
    }

    private static HttpResponseMessage JsonResponse(HttpStatusCode statusCode, object payload)
        => new(statusCode) { Content = JsonContent.Create(payload) };

    private static HttpResponseMessage ProblemResponse(HttpStatusCode statusCode, object payload)
        => new(statusCode) { Content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/problem+json") };

    [Fact]
    public async Task GetAsync_Success_ReturnsDeserializedPayload()
    {
        var (client, _) = CreateClient(_ => JsonResponse(HttpStatusCode.OK, new TestPayload("widget", 5)));

        var result = await client.Get<TestPayload>("/things/1", TestContext.Current.CancellationToken);

        result.ShouldBe(new TestPayload("widget", 5));
    }

    [Fact]
    public async Task GetAsync_ProblemJsonError_ThrowsWithParsedFields()
    {
        var (client, _) = CreateClient(_ => ProblemResponse(HttpStatusCode.NotFound, new
        {
            type = "https://example.test/not-found",
            title = "Not Found",
            detail = "thing 1 does not exist",
            errors = (Dictionary<string, string[]>?)null,
        }));

        var exception = await Should.ThrowAsync<ProblemDetailsException>(() => client.Get<TestPayload>("/things/1", TestContext.Current.CancellationToken));

        exception.StatusCode.ShouldBe(404);
        exception.Type.ShouldBe("https://example.test/not-found");
        exception.Title.ShouldBe("Not Found");
        exception.Message.ShouldBe("thing 1 does not exist");
    }

    [Fact]
    public async Task GetAsync_ErrorWithMalformedJsonBody_FallsBackToGenericException()
    {
        var (client, _) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            Content = new StringContent("not json", Encoding.UTF8, "application/problem+json"),
        });

        var exception = await Should.ThrowAsync<ProblemDetailsException>(() => client.Get<TestPayload>("/things/1", TestContext.Current.CancellationToken));

        exception.StatusCode.ShouldBe(500);
        exception.Type.ShouldBeNull();
        exception.Title.ShouldBeNull();
        exception.Message.ShouldBe("The request failed.");
    }

    [Fact]
    public async Task GetAsync_ErrorWithNonJsonContentType_SkipsBodyParsing()
    {
        var (client, _) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            Content = new StringContent("{\"detail\":\"should be ignored\"}", Encoding.UTF8, "text/plain"),
        });

        var exception = await Should.ThrowAsync<ProblemDetailsException>(() => client.Get<TestPayload>("/things/1", TestContext.Current.CancellationToken));

        exception.StatusCode.ShouldBe(500);
        exception.Message.ShouldBe("The request failed.");
    }

    [Fact]
    public async Task GetWithETagAsync_NoCachedETag_SendsNoConditionalHeader()
    {
        var (client, handler) = CreateClient(_ => JsonResponse(HttpStatusCode.OK, new TestPayload("widget", 5)));

        await client.GetWithETag<TestPayload>("/things/1", TestContext.Current.CancellationToken);

        handler.LastRequest!.HeaderValue("If-None-Match").ShouldBeNull();
    }

    [Fact]
    public async Task GetWithETagAsync_SecondCall_SendsCachedETagAsIfNoneMatch()
    {
        var (client, handler) = CreateClient(req =>
        {
            var response = JsonResponse(HttpStatusCode.OK, new TestPayload("widget", 5));
            response.Headers.ETag = new EntityTagHeaderValue("\"etag-1\"");
            return response;
        });

        await client.GetWithETag<TestPayload>("/things/1", TestContext.Current.CancellationToken);
        await client.GetWithETag<TestPayload>("/things/1", TestContext.Current.CancellationToken);

        handler.LastRequest!.HeaderValue("If-None-Match").ShouldBe("\"etag-1\"");
    }

    [Fact]
    public async Task GetWithETagAsync_NotModified_ReturnsDefaultWithoutThrowing()
    {
        var (client, _) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.NotModified));

        var result = await client.GetWithETag<TestPayload>("/things/1", TestContext.Current.CancellationToken);

        result.ShouldBeNull();
    }

    [Fact]
    public async Task PostAsync_WithResponseBody_ReturnsDeserializedResponse()
    {
        var (client, handler) = CreateClient(_ => JsonResponse(HttpStatusCode.Created, new TestPayload("created", 1)));

        var result = await client.Post<TestPayload, TestPayload>("/things", new TestPayload("widget", 5), TestContext.Current.CancellationToken);

        result.ShouldBe(new TestPayload("created", 1));
        handler.LastRequest!.Method.ShouldBe(HttpMethod.Post);
    }

    [Fact]
    public async Task PostAsync_WithoutResponseBody_CompletesSuccessfully()
    {
        var (client, handler) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.NoContent));

        await client.Post("/things", new TestPayload("widget", 5), TestContext.Current.CancellationToken);

        handler.LastRequest!.Method.ShouldBe(HttpMethod.Post);
    }

    [Fact]
    public async Task PostAsync_ErrorResponse_ThrowsProblemDetailsException()
    {
        var (client, _) = CreateClient(_ => ProblemResponse(HttpStatusCode.BadRequest, new { title = "Bad Request" }));

        await Should.ThrowAsync<ProblemDetailsException>(() => client.Post("/things", new TestPayload("widget", 5), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task PutAsync_NoCachedETag_SendsNoIfMatchHeader()
    {
        var (client, handler) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.OK));

        await client.Put("/things/1", new TestPayload("widget", 5), TestContext.Current.CancellationToken);

        handler.LastRequest!.HeaderValue("If-Match").ShouldBeNull();
    }

    [Fact]
    public async Task PutAsync_CachedETagFromPriorGet_SendsIfMatchHeader()
    {
        var (client, handler) = CreateClient(req =>
        {
            if (req.Method == HttpMethod.Get)
            {
                var getResponse = JsonResponse(HttpStatusCode.OK, new TestPayload("widget", 5));
                getResponse.Headers.ETag = new EntityTagHeaderValue("\"etag-1\"");
                return getResponse;
            }

            return new HttpResponseMessage(HttpStatusCode.OK);
        });

        await client.GetWithETag<TestPayload>("/things/1", TestContext.Current.CancellationToken);
        await client.Put("/things/1", new TestPayload("widget", 6), TestContext.Current.CancellationToken);

        handler.LastRequest!.HeaderValue("If-Match").ShouldBe("\"etag-1\"");
    }

    [Fact]
    public async Task DeleteAsync_Success_RemovesCachedETagForUri()
    {
        var (client, handler) = CreateClient(req =>
        {
            if (req.Method == HttpMethod.Get)
            {
                var getResponse = JsonResponse(HttpStatusCode.OK, new TestPayload("widget", 5));
                getResponse.Headers.ETag = new EntityTagHeaderValue("\"etag-1\"");
                return getResponse;
            }

            return new HttpResponseMessage(HttpStatusCode.OK);
        });

        await client.GetWithETag<TestPayload>("/things/1", TestContext.Current.CancellationToken);
        await client.Delete("/things/1", TestContext.Current.CancellationToken);
        await client.GetWithETag<TestPayload>("/things/1", TestContext.Current.CancellationToken);

        handler.LastRequest!.HeaderValue("If-None-Match").ShouldBeNull();
    }

    [Fact]
    public async Task DeleteAsync_ErrorResponse_ThrowsProblemDetailsException()
    {
        var (client, _) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.Forbidden));

        await Should.ThrowAsync<ProblemDetailsException>(() => client.Delete("/things/1", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task GetAsync_Success_InvokesOnResponseReceivedHookOnce()
    {
        var (client, _) = CreateClient(_ => JsonResponse(HttpStatusCode.OK, new TestPayload("widget", 5)));

        await client.Get<TestPayload>("/things/1", TestContext.Current.CancellationToken);

        client.ObservedResponses.Count.ShouldBe(1);
    }

    [Fact]
    public async Task GetAsync_ErrorResponse_StillInvokesOnResponseReceivedHook()
    {
        var (client, _) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError));

        await Should.ThrowAsync<ProblemDetailsException>(() => client.Get<TestPayload>("/things/1", TestContext.Current.CancellationToken));

        client.ObservedResponses.Count.ShouldBe(1);
        client.ObservedResponses[0].StatusCode.ShouldBe(HttpStatusCode.InternalServerError);
    }

    [Fact]
    public async Task GetWithETagAsync_NotModified_StillInvokesOnResponseReceivedHook()
    {
        var (client, _) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.NotModified));

        await client.GetWithETag<TestPayload>("/things/1", TestContext.Current.CancellationToken);

        client.ObservedResponses.Count.ShouldBe(1);
    }

    [Fact]
    public async Task PostAsync_WithResponseBody_InvokesOnResponseReceivedHookOnce()
    {
        var (client, _) = CreateClient(_ => JsonResponse(HttpStatusCode.Created, new TestPayload("created", 1)));

        await client.Post<TestPayload, TestPayload>("/things", new TestPayload("widget", 5), TestContext.Current.CancellationToken);

        client.ObservedResponses.Count.ShouldBe(1);
    }

    [Fact]
    public async Task PostAsync_WithoutResponseBody_InvokesOnResponseReceivedHookOnce()
    {
        var (client, _) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.NoContent));

        await client.Post("/things", new TestPayload("widget", 5), TestContext.Current.CancellationToken);

        client.ObservedResponses.Count.ShouldBe(1);
    }

    [Fact]
    public async Task PutAsync_InvokesOnResponseReceivedHookOnce()
    {
        var (client, _) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.OK));

        await client.Put("/things/1", new TestPayload("widget", 5), TestContext.Current.CancellationToken);

        client.ObservedResponses.Count.ShouldBe(1);
    }

    [Fact]
    public async Task DeleteAsync_InvokesOnResponseReceivedHookOnce()
    {
        var (client, _) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.OK));

        await client.Delete("/things/1", TestContext.Current.CancellationToken);

        client.ObservedResponses.Count.ShouldBe(1);
    }

    [Fact]
    public async Task GetWithETagAsync_UseConditionalRequestFalse_NeverSendsIfNoneMatchEvenWithCachedETag()
    {
        var (client, handler) = CreateClient(_ =>
        {
            var response = JsonResponse(HttpStatusCode.OK, new TestPayload("widget", 5));
            response.Headers.ETag = new EntityTagHeaderValue("\"etag-1\"");
            return response;
        });

        await client.GetWithETag<TestPayload>("/things/1", TestContext.Current.CancellationToken, useConditionalRequest: false);
        var result = await client.GetWithETag<TestPayload>("/things/1", TestContext.Current.CancellationToken, useConditionalRequest: false);

        handler.LastRequest!.HeaderValue("If-None-Match").ShouldBeNull();
        result.ShouldBe(new TestPayload("widget", 5));
    }

    [Fact]
    public async Task GetWithETagAsync_UseConditionalRequestFalse_StillCachesETagForSubsequentPut()
    {
        var (client, handler) = CreateClient(req =>
        {
            if (req.Method == HttpMethod.Get)
            {
                var getResponse = JsonResponse(HttpStatusCode.OK, new TestPayload("widget", 5));
                getResponse.Headers.ETag = new EntityTagHeaderValue("\"etag-1\"");
                return getResponse;
            }

            return new HttpResponseMessage(HttpStatusCode.OK);
        });

        await client.GetWithETag<TestPayload>("/things/1", TestContext.Current.CancellationToken, useConditionalRequest: false);
        await client.Put("/things/1", new TestPayload("widget", 6), TestContext.Current.CancellationToken);

        handler.LastRequest!.HeaderValue("If-Match").ShouldBe("\"etag-1\"");
    }

    [Fact]
    public async Task GetWithETagAsync_UseConditionalRequestTrue_DefaultBehaviorUnchanged()
    {
        var (client, handler) = CreateClient(req =>
        {
            var response = JsonResponse(HttpStatusCode.OK, new TestPayload("widget", 5));
            response.Headers.ETag = new EntityTagHeaderValue("\"etag-1\"");
            return response;
        });

        await client.GetWithETag<TestPayload>("/things/1", TestContext.Current.CancellationToken);
        await client.GetWithETag<TestPayload>("/things/1", TestContext.Current.CancellationToken);

        handler.LastRequest!.HeaderValue("If-None-Match").ShouldBe("\"etag-1\"");
    }

    [Fact]
    public async Task SendAsync_CustomRequest_Success_ReturnsResponseAndCachesETag()
    {
        var (client, handler) = CreateClient(req =>
        {
            var response = JsonResponse(HttpStatusCode.OK, new TestPayload("widget", 5));
            response.Headers.ETag = new EntityTagHeaderValue("\"etag-1\"");
            return response;
        });

        using var request = new HttpRequestMessage(HttpMethod.Post, "/things/upload") { Content = new StringContent("payload") };
        using var response = await client.Send(request, TestContext.Current.CancellationToken);
        var body = await TestApiClient.ReadJson<TestPayload>(response, TestContext.Current.CancellationToken);

        body.ShouldBe(new TestPayload("widget", 5));
        handler.CallCount.ShouldBe(1);

        // The cached ETag from the SendAsync call should be attached as If-Match on a subsequent PUT to the same URI.
        await client.Put("/things/upload", new TestPayload("widget", 6), TestContext.Current.CancellationToken);
        handler.LastRequest!.HeaderValue("If-Match").ShouldBe("\"etag-1\"");
    }

    [Fact]
    public async Task SendAsync_CustomRequest_ErrorResponse_ThrowsProblemDetailsException()
    {
        var (client, _) = CreateClient(_ => ProblemResponse(HttpStatusCode.BadRequest, new { title = "Bad Request" }));

        using var request = new HttpRequestMessage(HttpMethod.Post, "/things/upload") { Content = new StringContent("payload") };

        await Should.ThrowAsync<ProblemDetailsException>(() => client.Send(request, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SendAsync_CustomRequest_InvokesOnResponseReceivedHookOnce()
    {
        var (client, _) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.OK));

        using var request = new HttpRequestMessage(HttpMethod.Get, "/things/download");
        using var response = await client.Send(request, TestContext.Current.CancellationToken);

        client.ObservedResponses.Count.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_CustomRequest_InvokesOnRequestSendingHookOnce()
    {
        var (client, _) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.OK));

        using var request = new HttpRequestMessage(HttpMethod.Get, "/things/download");
        using var response = await client.Send(request, TestContext.Current.CancellationToken);

        client.ObservedRequests.Count.ShouldBe(1);
    }

    [Fact]
    public async Task GetAsync_InvokesOnRequestSendingHookOnce()
    {
        var (client, _) = CreateClient(_ => JsonResponse(HttpStatusCode.OK, new TestPayload("widget", 5)));

        await client.Get<TestPayload>("/things/1", TestContext.Current.CancellationToken);

        client.ObservedRequests.Count.ShouldBe(1);
    }

    [Fact]
    public async Task GetWithETagAsync_InvokesOnRequestSendingHookOnce()
    {
        var (client, _) = CreateClient(_ => JsonResponse(HttpStatusCode.OK, new TestPayload("widget", 5)));

        await client.GetWithETag<TestPayload>("/things/1", TestContext.Current.CancellationToken);

        client.ObservedRequests.Count.ShouldBe(1);
    }

    [Fact]
    public async Task PostAsync_WithResponseBody_InvokesOnRequestSendingHookOnce()
    {
        var (client, _) = CreateClient(_ => JsonResponse(HttpStatusCode.Created, new TestPayload("created", 1)));

        await client.Post<TestPayload, TestPayload>("/things", new TestPayload("widget", 5), TestContext.Current.CancellationToken);

        client.ObservedRequests.Count.ShouldBe(1);
    }

    [Fact]
    public async Task PostAsync_WithoutResponseBody_InvokesOnRequestSendingHookOnce()
    {
        var (client, _) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.NoContent));

        await client.Post("/things", new TestPayload("widget", 5), TestContext.Current.CancellationToken);

        client.ObservedRequests.Count.ShouldBe(1);
    }

    [Fact]
    public async Task PutAsync_InvokesOnRequestSendingHookOnce()
    {
        var (client, _) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.OK));

        await client.Put("/things/1", new TestPayload("widget", 5), TestContext.Current.CancellationToken);

        client.ObservedRequests.Count.ShouldBe(1);
    }

    [Fact]
    public async Task DeleteAsync_InvokesOnRequestSendingHookOnce()
    {
        var (client, _) = CreateClient(_ => new HttpResponseMessage(HttpStatusCode.OK));

        await client.Delete("/things/1", TestContext.Current.CancellationToken);

        client.ObservedRequests.Count.ShouldBe(1);
    }

    [Fact]
    public async Task OnRequestSendingAsync_CanAttachHeaderThatReachesTheWire()
    {
        var (client, handler) = CreateClient(_ => JsonResponse(HttpStatusCode.OK, new TestPayload("widget", 5)));
        client.MutateRequest = request => request.Headers.TryAddWithoutValidation("Authorization", "Bearer test-token");

        await client.Get<TestPayload>("/things/1", TestContext.Current.CancellationToken);

        handler.LastRequest!.HeaderValue("Authorization").ShouldBe("Bearer test-token");
    }
}
