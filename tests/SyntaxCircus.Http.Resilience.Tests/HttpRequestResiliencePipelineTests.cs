using System.Net.Http.Headers;

namespace SyntaxCircus.Http.Resilience.Tests;

public class HttpRequestResiliencePipelineTests
{
    [Fact]
    public void Constructor_AcceptsAllPositiveDurationsAllowedByTaskOneContract()
    {
        var duration = TimeSpan.FromTicks(1);

        var pipeline = new HttpRequestResiliencePipeline("test", new HttpRequestResilienceOptions
        {
            TotalRequestTimeout = duration,
            BackoffBaseDelay = duration,
            MaximumDelay = duration,
            CircuitSamplingDuration = duration,
            CircuitBreakDuration = duration,
        });

        pipeline.ShouldNotBeNull();
    }

    [Fact]
    public async Task SendAsync_RebuildsAndDisposesEveryRetriedAttempt()
    {
        var timeProvider = new ManualTimeProvider();
        var requests = new List<HttpRequestMessage>();
        var attempts = new List<int>();
        var observerStatuses = new List<HttpStatusCode>();
        var retryEvents = new List<HttpRetryTelemetry>();
        var firstRequestContent = new TrackingContent();
        var firstResponseContent = new TrackingContent();
        var finalResponseContent = new TrackingContent();
        var pipeline = CreatePipeline(timeProvider, onRetry: (telemetry, _) =>
        {
            retryEvents.Add(telemetry);
            return ValueTask.CompletedTask;
        });

        var operation = pipeline.SendAsync(
            (attempt, _) =>
            {
                attempts.Add(attempt);
                var request = new HttpRequestMessage(HttpMethod.Get, $"https://example.test/{attempt}")
                {
                    Content = attempt == 1 ? firstRequestContent : null,
                };
                requests.Add(request);
                return ValueTask.FromResult(request);
            },
            (_, _, _) => Task.FromResult(attempts.Count == 1
                ? new HttpResponseMessage(HttpStatusCode.ServiceUnavailable) { Content = firstResponseContent }
                : new HttpResponseMessage(HttpStatusCode.OK) { Content = finalResponseContent }),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            (response, _) =>
            {
                observerStatuses.Add(response.StatusCode);
                return ValueTask.CompletedTask;
            },
            TestContext.Current.CancellationToken);

        await AdvanceRetryAsync(operation, timeProvider, TimeSpan.FromSeconds(1));
        var final = await operation;

        attempts.ShouldBe([1, 2]);
        requests.Count.ShouldBe(2);
        requests[0].ShouldNotBeSameAs(requests[1]);
        firstRequestContent.Disposed.ShouldBeTrue();
        firstResponseContent.Disposed.ShouldBeTrue();
        observerStatuses.ShouldBe([HttpStatusCode.ServiceUnavailable, HttpStatusCode.OK]);
        retryEvents.ShouldBe([
            new HttpRetryTelemetry(
                "test",
                1,
                HttpStatusCode.ServiceUnavailable,
                HttpResilienceFailureCategory.HttpStatus,
                TimeSpan.FromSeconds(1)),
        ]);
        final.StatusCode.ShouldBe(HttpStatusCode.OK);
        finalResponseContent.Disposed.ShouldBeFalse();

        final.Dispose();
        finalResponseContent.Disposed.ShouldBeTrue();
    }

    [Fact]
    public async Task SendAsync_TransportFailureThenSuccess_RetriesWithFreshRequest()
    {
        var timeProvider = new ManualTimeProvider();
        var requests = new List<HttpRequestMessage>();
        var sends = 0;
        var pipeline = CreatePipeline(timeProvider);

        var operation = pipeline.SendAsync(
            (attempt, _) =>
            {
                var request = new HttpRequestMessage(HttpMethod.Get, $"https://example.test/{attempt}");
                requests.Add(request);
                return ValueTask.FromResult(request);
            },
            (_, _, _) => ++sends == 1
                ? Task.FromException<HttpResponseMessage>(new HttpRequestException("transient"))
                : Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        await AdvanceRetryAsync(operation, timeProvider, TimeSpan.FromSeconds(1));
        using var response = await operation;

        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        sends.ShouldBe(2);
        requests.Count.ShouldBe(2);
        requests[0].ShouldNotBeSameAs(requests[1]);
    }

    [Fact]
    public async Task SendAsync_TooManyRequestsDeltaRetryAfter_UsesServerDelayWithoutJitter()
    {
        var timeProvider = new ManualTimeProvider();
        var retryEvents = new List<HttpRetryTelemetry>();
        var jitterCalls = 0;
        var sends = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            maximumDelay: TimeSpan.FromSeconds(5),
            jitterProvider: () => { jitterCalls++; return 0.5; },
            onRetry: (telemetry, _) => { retryEvents.Add(telemetry); return ValueTask.CompletedTask; });

        var operation = SendAsync(pipeline, (_, _, _) =>
        {
            sends++;
            if (sends > 1)
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            }

            var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
            response.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.FromSeconds(3));
            return Task.FromResult(response);
        });

        await AdvanceRetryAsync(operation, timeProvider, TimeSpan.FromSeconds(3));
        using var final = await operation;

        sends.ShouldBe(2);
        jitterCalls.ShouldBe(0);
        retryEvents.Single().Delay.ShouldBe(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task SendAsync_TooManyRequestsHttpDateRetryAfter_UsesTimeProviderClock()
    {
        var timeProvider = new ManualTimeProvider(new DateTimeOffset(2026, 8, 27, 12, 0, 0, TimeSpan.Zero));
        var retryEvents = new List<HttpRetryTelemetry>();
        var sends = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            maximumDelay: TimeSpan.FromSeconds(10),
            onRetry: (telemetry, _) => { retryEvents.Add(telemetry); return ValueTask.CompletedTask; });

        var operation = SendAsync(pipeline, (_, _, _) =>
        {
            sends++;
            if (sends > 1)
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            }

            var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
            response.Headers.RetryAfter = new RetryConditionHeaderValue(timeProvider.GetUtcNow().AddSeconds(4));
            return Task.FromResult(response);
        });

        await AdvanceRetryAsync(operation, timeProvider, TimeSpan.FromSeconds(4));
        using var final = await operation;

        sends.ShouldBe(2);
        retryEvents.Single().Delay.ShouldBe(TimeSpan.FromSeconds(4));
    }

    [Theory]
    [InlineData(HttpStatusCode.RequestTimeout)]
    [InlineData(HttpStatusCode.TooManyRequests)]
    [InlineData(HttpStatusCode.InternalServerError)]
    [InlineData(HttpStatusCode.BadGateway)]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    [InlineData(HttpStatusCode.GatewayTimeout)]
    public async Task SendAsync_DocumentedTransientStatus_Retries(HttpStatusCode statusCode)
    {
        var timeProvider = new ManualTimeProvider();
        var sends = 0;
        var pipeline = CreatePipeline(timeProvider);
        var operation = SendAsync(pipeline, (_, _, _) => Task.FromResult(
            ++sends == 1 ? new HttpResponseMessage(statusCode) : new HttpResponseMessage(HttpStatusCode.OK)));

        await AdvanceRetryAsync(operation, timeProvider, TimeSpan.FromSeconds(1));
        using var final = await operation;

        sends.ShouldBe(2);
        final.StatusCode.ShouldBe(HttpStatusCode.OK);
    }

    [Fact]
    public async Task SendAsync_UndocumentedServerStatus_DoesNotRetry()
    {
        var sends = 0;
        var pipeline = CreatePipeline(new ManualTimeProvider());

        using var response = await SendAsync(pipeline, (_, _, _) =>
            Task.FromResult(new HttpResponseMessage(++sends == 1 ? HttpStatusCode.NotImplemented : HttpStatusCode.OK)));

        response.StatusCode.ShouldBe(HttpStatusCode.NotImplemented);
        sends.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_CustomIncludedStatus_RetriesAndFeedsCircuitClassification()
    {
        var timeProvider = new ManualTimeProvider();
        var circuitEvents = new List<HttpCircuitTelemetry>();
        var sends = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 1,
            circuitMinimumThroughput: 2,
            retryableStatusCodes: new HashSet<HttpStatusCode> { HttpStatusCode.NotImplemented },
            onCircuit: (telemetry, _) => { circuitEvents.Add(telemetry); return ValueTask.CompletedTask; });

        for (var i = 0; i < 2; i++)
        {
            using var response = await SendAsync(pipeline, (_, _, _) =>
                Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotImplemented)));
        }

        await Should.ThrowAsync<HttpCircuitOpenException>(() => SendAsync(pipeline, (_, _, _) =>
        {
            sends++;
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
        }));

        sends.ShouldBe(0);
        circuitEvents.Single().FailureCategory.ShouldBe(HttpResilienceFailureCategory.HttpStatus);
        circuitEvents.Single().StatusCode.ShouldBe(HttpStatusCode.NotImplemented);
    }

    [Fact]
    public async Task SendAsync_CustomExcludedStatus_DoesNotRetryOrOpenCircuit()
    {
        var sends = 0;
        var pipeline = CreatePipeline(
            new ManualTimeProvider(),
            maxAttempts: 2,
            circuitMinimumThroughput: 2,
            retryableStatusCodes: new HashSet<HttpStatusCode>());

        for (var i = 0; i < 3; i++)
        {
            using var response = await SendAsync(pipeline, (_, _, _) =>
            {
                sends++;
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
            });
        }

        sends.ShouldBe(3);
    }

    [Fact]
    public async Task SendAsync_ExcludedTransportCategory_DoesNotRetryOrOpenCircuit()
    {
        var sends = 0;
        var pipeline = CreatePipeline(
            new ManualTimeProvider(),
            maxAttempts: 2,
            circuitMinimumThroughput: 2,
            retryableExceptionCategories: new HashSet<HttpResilienceFailureCategory> { HttpResilienceFailureCategory.Timeout });

        for (var i = 0; i < 3; i++)
        {
            await Should.ThrowAsync<HttpRequestException>(() => SendAsync(pipeline, (_, _, _) =>
            {
                sends++;
                return Task.FromException<HttpResponseMessage>(new HttpRequestException("excluded"));
            }));
        }

        sends.ShouldBe(3);
    }

    [Fact]
    public async Task SendAsync_ClassifierSetsAreSnapshottedAtConstruction()
    {
        var timeProvider = new ManualTimeProvider();
        var statusCodes = new HashSet<HttpStatusCode> { HttpStatusCode.NotImplemented };
        var categories = new HashSet<HttpResilienceFailureCategory> { HttpResilienceFailureCategory.Transport };
        var sends = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 2,
            retryableStatusCodes: statusCodes,
            retryableExceptionCategories: categories);
        statusCodes.Clear();
        statusCodes.Add(HttpStatusCode.ServiceUnavailable);
        categories.Clear();

        var operation = SendAsync(pipeline, (_, _, _) => Task.FromResult(
            ++sends == 1
                ? new HttpResponseMessage(HttpStatusCode.NotImplemented)
                : new HttpResponseMessage(HttpStatusCode.OK)));

        await AdvanceRetryAsync(operation, timeProvider, TimeSpan.FromSeconds(1));
        using var response = await operation;

        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        sends.ShouldBe(2);
    }

    [Fact]
    public async Task SendAsync_NotReplayable_ReturnsFirstTransientResponse()
    {
        var sends = 0;
        var responseContent = new TrackingContent();
        var pipeline = CreatePipeline(new ManualTimeProvider());

        var response = await SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(++sends == 1 ? HttpStatusCode.ServiceUnavailable : HttpStatusCode.OK)
            {
                Content = responseContent,
            }),
            HttpRequestReplaySafety.NotReplayable);

        response.StatusCode.ShouldBe(HttpStatusCode.ServiceUnavailable);
        sends.ShouldBe(1);
        responseContent.Disposed.ShouldBeFalse();

        response.Dispose();
        responseContent.Disposed.ShouldBeTrue();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SendAsync_ResponseObserverFailure_DoesNotRetryDisposesResponseAndPreservesException(bool timeoutFailure)
    {
        var sends = 0;
        var responseContents = new List<TrackingContent>();
        Exception observerException = timeoutFailure
            ? new TimeoutException("observer timeout")
            : new HttpRequestException("observer transport");
        var pipeline = new HttpRequestResiliencePipeline("test", new HttpRequestResilienceOptions
        {
            MaxAttempts = 2,
            TotalRequestTimeout = TimeSpan.FromSeconds(5),
            BackoffBaseDelay = TimeSpan.FromMilliseconds(1),
            MaximumDelay = TimeSpan.FromMilliseconds(1),
            CircuitMinimumThroughput = 20,
            TimeProvider = TimeProvider.System,
            JitterProvider = () => 0,
        });

        var actual = await Should.ThrowAsync<Exception>(() => pipeline.SendAsync(
            (attempt, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, $"https://example.test/{attempt}")),
            (_, _, _) =>
            {
                sends++;
                var content = new TrackingContent();
                responseContents.Add(content);
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = content });
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            (_, _) => throw observerException,
            TestContext.Current.CancellationToken));

        actual.ShouldBeSameAs(observerException);
        responseContents[0].Disposed.ShouldBeTrue();
        sends.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_ResponseObserverFailure_PreservesOriginalWhenRequestAndResponseDisposalThrow()
    {
        var observerSends = 0;
        var followingSends = 0;
        var requestContents = new List<ThrowingDisposeContent>();
        var responseContents = new List<ThrowingDisposeContent>();
        using var observerCancellation = new CancellationTokenSource();
        var observerException = new OperationCanceledException("observer sentinel", observerCancellation.Token);
        var pipeline = new HttpRequestResiliencePipeline("test", new HttpRequestResilienceOptions
        {
            MaxAttempts = 2,
            TotalRequestTimeout = TimeSpan.FromSeconds(5),
            BackoffBaseDelay = TimeSpan.FromMilliseconds(1),
            MaximumDelay = TimeSpan.FromMilliseconds(1),
            CircuitFailureRatio = 1,
            CircuitMinimumThroughput = 2,
            CircuitSamplingDuration = TimeSpan.FromSeconds(30),
            CircuitBreakDuration = TimeSpan.FromSeconds(5),
            TimeProvider = TimeProvider.System,
            JitterProvider = () => 0,
        });

        using (var primer = await SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)),
            HttpRequestReplaySafety.NotReplayable))
        {
            primer.StatusCode.ShouldBe(HttpStatusCode.ServiceUnavailable);
        }

        var actual = await Record.ExceptionAsync(() => pipeline.SendAsync(
            (attempt, _) =>
            {
                var content = new ThrowingDisposeContent(new HttpRequestException("request cleanup"));
                requestContents.Add(content);
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, $"https://example.test/{attempt}")
                {
                    Content = content,
                });
            },
            (_, _, _) =>
            {
                observerSends++;
                var content = new ThrowingDisposeContent(new HttpRequestException("response cleanup"));
                responseContents.Add(content);
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = content });
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            (_, _) => throw observerException,
            TestContext.Current.CancellationToken));

        HttpResponseMessage? followingResponse = null;
        var followingFailure = await Record.ExceptionAsync(async () =>
        {
            followingResponse = await SendAsync(
                pipeline,
                (_, _, _) =>
                {
                    followingSends++;
                    return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
                },
                HttpRequestReplaySafety.NotReplayable);
        });

        try
        {
            actual.ShouldBeSameAs(observerException);
            actual.ShouldBeOfType<OperationCanceledException>().CancellationToken.ShouldBe(observerCancellation.Token);
            observerSends.ShouldBe(1);
            requestContents.Count.ShouldBe(1);
            responseContents.Count.ShouldBe(1);
            requestContents[0].DisposeAttempted.ShouldBeTrue();
            responseContents[0].DisposeAttempted.ShouldBeTrue();
            followingFailure.ShouldBeNull();
            followingSends.ShouldBe(1);
            followingResponse.ShouldNotBeNull().StatusCode.ShouldBe(HttpStatusCode.OK);
        }
        finally
        {
            followingResponse?.Dispose();
        }
    }

    [Fact]
    public async Task SendAsync_MaxValueTotalBudget_HasNoDeadline()
    {
        var sends = 0;
        var attemptToken = default(CancellationToken);
        var callerToken = TestContext.Current.CancellationToken;
        var pipeline = CreatePipeline(
            new ManualTimeProvider(),
            maxAttempts: 1,
            totalTimeout: TimeSpan.MaxValue);

        using var response = await pipeline.SendAsync(
            (attempt, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, $"https://example.test/{attempt}")),
            (_, _, cancellationToken) =>
            {
                sends++;
                attemptToken = cancellationToken;
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: callerToken);

        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        sends.ShouldBe(1);
        attemptToken.ShouldBe(callerToken);
    }

    [Fact]
    public async Task SendAsync_TotalBudgetIncludesAttemptsAndRetryDelays()
    {
        var timeProvider = new ManualTimeProvider();
        var sends = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            totalTimeout: TimeSpan.FromSeconds(1),
            backoffDelay: TimeSpan.FromMilliseconds(500));

        var operation = SendAsync(pipeline, (_, _, _) =>
        {
            sends++;
            timeProvider.Advance(TimeSpan.FromMilliseconds(600));
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
        });

        await WaitForTimerCountAsync(timeProvider, 2);
        timeProvider.Advance(TimeSpan.FromMilliseconds(400));

        var exception = await Should.ThrowAsync<HttpRequestTimeoutException>(() => operation);
        exception.PipelineName.ShouldBe("test");
        exception.Timeout.ShouldBe(TimeSpan.FromSeconds(1));
        sends.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_RetryAfterExceedsBudget_ThrowsTimeoutWithoutAnotherSend()
    {
        var timeProvider = new ManualTimeProvider();
        var sends = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            totalTimeout: TimeSpan.FromSeconds(2),
            maximumDelay: TimeSpan.FromSeconds(20));

        var operation = SendAsync(pipeline, (_, _, _) =>
        {
            sends++;
            var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
            response.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.FromSeconds(10));
            return Task.FromResult(response);
        });

        await WaitForTimerCountAsync(timeProvider, 2);
        timeProvider.Advance(TimeSpan.FromSeconds(2));

        await Should.ThrowAsync<HttpRequestTimeoutException>(() => operation);
        sends.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_LogicalBudgetExpiryEmitsExactlyOneTimeoutEventWithExactFields()
    {
        var timeProvider = new ManualTimeProvider();
        var events = new List<HttpTimeoutTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            totalTimeout: TimeSpan.FromMilliseconds(500),
            onTimeout: (telemetry, _) => { events.Add(telemetry); return ValueTask.CompletedTask; });
        var operation = SendAsync(pipeline, (_, _, _) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));

        await WaitForTimerCountAsync(timeProvider, 2);
        timeProvider.Advance(TimeSpan.FromMilliseconds(500));

        await Should.ThrowAsync<HttpRequestTimeoutException>(() => operation);
        events.ShouldBe([
            new HttpTimeoutTelemetry(
                "test",
                HttpResilienceFailureCategory.Timeout,
                TimeSpan.FromMilliseconds(500)),
        ]);
    }

    [Fact]
    public async Task SendAsync_ThrowingTimeoutCallbackDoesNotReplaceTimeoutOrCallerCancellation()
    {
        var callbackCount = 0;
        ValueTask ThrowingTimeout(HttpTimeoutTelemetry _, CancellationToken __)
        {
            callbackCount++;
            return ValueTask.FromException(new InvalidOperationException("telemetry"));
        }

        var timeProvider = new ManualTimeProvider();
        var pipeline = CreatePipeline(
            timeProvider,
            totalTimeout: TimeSpan.FromMilliseconds(500),
            onTimeout: ThrowingTimeout);
        var timeoutOperation = SendAsync(pipeline, (_, _, _) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        await WaitForTimerCountAsync(timeProvider, 2);
        timeProvider.Advance(TimeSpan.FromMilliseconds(500));
        await Should.ThrowAsync<HttpRequestTimeoutException>(() => timeoutOperation);

        using var cancellation = new CancellationTokenSource();
        var callerException = new OperationCanceledException("caller", cancellation.Token);
        var actual = await Should.ThrowAsync<OperationCanceledException>(() => SendAsyncWithCancellation(
            pipeline,
            (_, _, _) =>
            {
                cancellation.Cancel();
                return Task.FromException<HttpResponseMessage>(callerException);
            },
            cancellation.Token));

        actual.CancellationToken.ShouldBe(cancellation.Token);
        callbackCount.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_CallerCancellationPreservesCallerTokenAndDoesNotRetry()
    {
        using var cancellation = new CancellationTokenSource();
        var sends = 0;
        var pipeline = CreatePipeline(new ManualTimeProvider());

        var callerException = new OperationCanceledException("caller", cancellation.Token);
        var exception = await Should.ThrowAsync<OperationCanceledException>(() => SendAsyncWithCancellation(pipeline, (_, _, _) =>
        {
            sends++;
            cancellation.Cancel();
            return Task.FromException<HttpResponseMessage>(callerException);
        }, cancellation.Token));

        exception.CancellationToken.ShouldBe(cancellation.Token);
        sends.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_SenderCancelsCallerThenReturnsRetryableResponse_ObservesDisposesAndDoesNotPolluteRetryOrCircuit()
    {
        using var cancellation = new CancellationTokenSource();
        var sends = 0;
        var observerCalls = 0;
        var followingSends = 0;
        var retryEvents = new List<HttpRetryTelemetry>();
        var circuitEvents = new List<HttpCircuitTelemetry>();
        var responseContent = new ThrowingDisposeContent(new InvalidOperationException("response cleanup"));
        var pipeline = CreatePipeline(
            new ManualTimeProvider(),
            circuitMinimumThroughput: 2,
            onRetry: (telemetry, _) => { retryEvents.Add(telemetry); return ValueTask.CompletedTask; },
            onCircuit: (telemetry, _) => { circuitEvents.Add(telemetry); return ValueTask.CompletedTask; });

        var actual = await Should.ThrowAsync<OperationCanceledException>(() => pipeline.SendAsync(
            (attempt, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, $"https://example.test/{attempt}")),
            (_, _, _) =>
            {
                sends++;
                cancellation.Cancel();
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Content = responseContent,
                });
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            (_, _) =>
            {
                observerCalls++;
                return ValueTask.CompletedTask;
            },
            cancellation.Token));

        using var following = await SendAsync(
            pipeline,
            (_, _, _) =>
            {
                followingSends++;
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            },
            HttpRequestReplaySafety.NotReplayable);

        actual.CancellationToken.ShouldBe(cancellation.Token);
        sends.ShouldBe(1);
        observerCalls.ShouldBe(1);
        responseContent.DisposeAttempted.ShouldBeTrue();
        retryEvents.ShouldBeEmpty();
        circuitEvents.ShouldBeEmpty();
        followingSends.ShouldBe(1);
        following.StatusCode.ShouldBe(HttpStatusCode.OK);
    }

    [Fact]
    public async Task SendAsync_SenderCancelsCallerThenObserverThrows_CallerCancellationWinsAndCleanupIsAttempted()
    {
        using var cancellation = new CancellationTokenSource();
        var observerCalls = 0;
        var responseContent = new ThrowingDisposeContent(new InvalidOperationException("response cleanup"));
        var pipeline = CreatePipeline(new ManualTimeProvider());

        var actual = await Should.ThrowAsync<OperationCanceledException>(() => pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/cancel")),
            (_, _, _) =>
            {
                cancellation.Cancel();
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Content = responseContent,
                });
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            (_, _) =>
            {
                observerCalls++;
                throw new InvalidOperationException("observer");
            },
            cancellation.Token));

        actual.CancellationToken.ShouldBe(cancellation.Token);
        observerCalls.ShouldBe(1);
        responseContent.DisposeAttempted.ShouldBeTrue();
    }

    [Fact]
    public async Task SendAsync_SenderCancelsCallerThenThrowsTransport_CallerCancellationWinsWithoutRetryOrCircuitPollution()
    {
        using var cancellation = new CancellationTokenSource();
        var sends = 0;
        var followingSends = 0;
        var retryEvents = new List<HttpRetryTelemetry>();
        var circuitEvents = new List<HttpCircuitTelemetry>();
        var pipeline = CreatePipeline(
            new ManualTimeProvider(),
            circuitMinimumThroughput: 2,
            onRetry: (telemetry, _) => { retryEvents.Add(telemetry); return ValueTask.CompletedTask; },
            onCircuit: (telemetry, _) => { circuitEvents.Add(telemetry); return ValueTask.CompletedTask; });

        var actual = await Should.ThrowAsync<OperationCanceledException>(() => SendAsyncWithCancellation(
            pipeline,
            (_, _, _) =>
            {
                sends++;
                cancellation.Cancel();
                return Task.FromException<HttpResponseMessage>(new HttpRequestException("transport"));
            },
            cancellation.Token));

        using var following = await SendAsync(
            pipeline,
            (_, _, _) =>
            {
                followingSends++;
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            },
            HttpRequestReplaySafety.NotReplayable);

        actual.CancellationToken.ShouldBe(cancellation.Token);
        sends.ShouldBe(1);
        retryEvents.ShouldBeEmpty();
        circuitEvents.ShouldBeEmpty();
        followingSends.ShouldBe(1);
        following.StatusCode.ShouldBe(HttpStatusCode.OK);
    }

    [Fact]
    public async Task SendAsync_ThrowingRequestCleanupCannotReplaceCallerCancellationAndPipelineRemainsHealthy()
    {
        using var cancellation = new CancellationTokenSource();
        var callerException = new OperationCanceledException("caller", cancellation.Token);
        var throwingContent = new ThrowingDisposeContent(new InvalidOperationException("request cleanup"));
        var circuitEvents = new List<HttpCircuitTelemetry>();
        var pipeline = CreatePipeline(
            new ManualTimeProvider(),
            circuitMinimumThroughput: 2,
            onCircuit: (telemetry, _) => { circuitEvents.Add(telemetry); return ValueTask.CompletedTask; });

        var actual = await Should.ThrowAsync<OperationCanceledException>(() => pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/cancel")
            {
                Content = throwingContent,
            }),
            (_, _, _) =>
            {
                cancellation.Cancel();
                return Task.FromException<HttpResponseMessage>(callerException);
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: cancellation.Token));

        actual.CancellationToken.ShouldBe(cancellation.Token);
        throwingContent.DisposeAttempted.ShouldBeTrue();
        circuitEvents.ShouldBeEmpty();

        using var response = await SendAsync(pipeline, (_, _, _) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));
        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        circuitEvents.ShouldBeEmpty();
    }

    [Fact]
    public async Task SendAsync_ThrowingPendingResponseCleanupCannotReplaceCallerCancellationAndPipelineRemainsHealthy()
    {
        using var cancellation = new CancellationTokenSource();
        var callerException = new OperationCanceledException("caller", cancellation.Token);
        var throwingContent = new ThrowingDisposeContent(new InvalidOperationException("response cleanup"));
        var circuitEvents = new List<HttpCircuitTelemetry>();
        var pipeline = CreatePipeline(
            new ManualTimeProvider(),
            circuitMinimumThroughput: 2,
            onCircuit: (telemetry, _) => { circuitEvents.Add(telemetry); return ValueTask.CompletedTask; });

        var actual = await Should.ThrowAsync<OperationCanceledException>(() => pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/cancel")),
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Content = throwingContent,
                }),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            (_, _) =>
            {
                cancellation.Cancel();
                return ValueTask.FromException(callerException);
            },
            cancellation.Token));

        actual.CancellationToken.ShouldBe(cancellation.Token);
        throwingContent.DisposeAttempted.ShouldBeTrue();
        circuitEvents.ShouldBeEmpty();

        using var response = await SendAsync(pipeline, (_, _, _) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));
        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        circuitEvents.ShouldBeEmpty();
    }

    [Fact]
    public async Task SendAsync_ExhaustedTransportRetriesThrowsLastHttpRequestException()
    {
        var timeProvider = new ManualTimeProvider();
        var first = new HttpRequestException("first");
        var last = new HttpRequestException("last");
        var sends = 0;
        var pipeline = CreatePipeline(timeProvider, maxAttempts: 2);
        var operation = SendAsync(pipeline, (_, _, _) =>
            Task.FromException<HttpResponseMessage>(++sends == 1 ? first : last));

        await AdvanceRetryAsync(operation, timeProvider, TimeSpan.FromSeconds(1));

        var exception = await Should.ThrowAsync<HttpRequestException>(() => operation);
        exception.ShouldBeSameAs(last);
        sends.ShouldBe(2);
    }

    [Theory]
    [InlineData(-0.1)]
    [InlineData(1.0)]
    [InlineData(double.NaN)]
    public async Task SendAsync_InvalidJitterFailsBeforeAnyDelay(double jitter)
    {
        var timeProvider = new ManualTimeProvider();
        var responseContent = new TrackingContent();
        var pipeline = CreatePipeline(timeProvider, jitterProvider: () => jitter);

        var exception = await Should.ThrowAsync<InvalidOperationException>(() => SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
            {
                Content = responseContent,
            })));

        exception.Message.ShouldContain("JitterProvider");
        timeProvider.CreatedTimerCount.ShouldBe(1);
        responseContent.Disposed.ShouldBeTrue();
    }

    [Fact]
    public async Task SendAsync_CircuitOpensAndRejectsBeforeRequestConstruction()
    {
        var timeProvider = new ManualTimeProvider();
        var factoryCalls = 0;
        var pipeline = CreatePipeline(timeProvider, maxAttempts: 1, circuitMinimumThroughput: 2);

        for (var i = 0; i < 2; i++)
        {
            using var response = await pipeline.SendAsync(
                (_, _) => { factoryCalls++; return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test")); },
                (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)),
                HttpCompletionOption.ResponseHeadersRead,
                HttpRequestReplaySafety.Replayable,
                cancellationToken: TestContext.Current.CancellationToken);
        }

        var exception = await Should.ThrowAsync<HttpCircuitOpenException>(() => pipeline.SendAsync(
            (_, _) => { factoryCalls++; return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test")); },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken));

        factoryCalls.ShouldBe(2);
        exception.PipelineName.ShouldBe("test");
        exception.RetryAfter.ShouldNotBeNull();
        exception.RetryAfter.Value.ShouldBeGreaterThan(TimeSpan.Zero);
        exception.RetryAfter.Value.ShouldBeLessThanOrEqualTo(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task SendAsync_FailuresAgeOutOfSubPollyMinimumCircuitSamplingWindow()
    {
        var timeProvider = new ManualTimeProvider();
        var factoryCalls = 0;
        var sends = 0;
        var circuitEvents = new List<HttpCircuitTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 1,
            circuitMinimumThroughput: 2,
            circuitSamplingDuration: TimeSpan.FromMilliseconds(100),
            onCircuit: (telemetry, _) => { circuitEvents.Add(telemetry); return ValueTask.CompletedTask; });

        using var first = await pipeline.SendAsync(
            (_, _) =>
            {
                factoryCalls++;
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/first"));
            },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(++sends <= 2
                ? HttpStatusCode.ServiceUnavailable
                : HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        timeProvider.Advance(TimeSpan.FromMilliseconds(200));

        using var second = await pipeline.SendAsync(
            (_, _) =>
            {
                factoryCalls++;
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/second"));
            },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(++sends <= 2
                ? HttpStatusCode.ServiceUnavailable
                : HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        using var third = await pipeline.SendAsync(
            (_, _) =>
            {
                factoryCalls++;
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/third"));
            },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(++sends <= 2
                ? HttpStatusCode.ServiceUnavailable
                : HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        first.StatusCode.ShouldBe(HttpStatusCode.ServiceUnavailable);
        second.StatusCode.ShouldBe(HttpStatusCode.ServiceUnavailable);
        third.StatusCode.ShouldBe(HttpStatusCode.OK);
        factoryCalls.ShouldBe(3);
        circuitEvents.ShouldBeEmpty();
    }

    [Fact]
    public async Task SendAsync_MaximumBreakDurationRemainsOpenWithOneTickSamplingWindow()
    {
        var timeProvider = new ManualTimeProvider();
        var factoryCalls = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 1,
            circuitMinimumThroughput: 2,
            circuitSamplingDuration: TimeSpan.FromTicks(1),
            circuitBreakDuration: TimeSpan.MaxValue);

        for (var i = 0; i < 2; i++)
        {
            using var response = await pipeline.SendAsync(
                (_, _) =>
                {
                    factoryCalls++;
                    return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/failure"));
                },
                (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)),
                HttpCompletionOption.ResponseHeadersRead,
                HttpRequestReplaySafety.Replayable,
                cancellationToken: TestContext.Current.CancellationToken);
        }

        timeProvider.Advance(TimeSpan.FromDays(3));

        HttpCircuitOpenException? rejection = null;
        try
        {
            using var unexpectedResponse = await pipeline.SendAsync(
                (_, _) =>
                {
                    factoryCalls++;
                    return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/probe"));
                },
                (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
                HttpCompletionOption.ResponseHeadersRead,
                HttpRequestReplaySafety.Replayable,
                cancellationToken: TestContext.Current.CancellationToken);
        }
        catch (HttpCircuitOpenException exception)
        {
            rejection = exception;
        }

        rejection.ShouldNotBeNull();
        rejection.RetryAfter.ShouldNotBeNull();
        rejection.RetryAfter.Value.ShouldBe(TimeSpan.MaxValue - TimeSpan.FromDays(3));
        factoryCalls.ShouldBe(2);
    }

    [Fact]
    public async Task SendAsync_OneTickSamplingFailuresStillAgeAfterOldTimestampSaturationPoint()
    {
        var timeProvider = new ManualTimeProvider();
        var factoryCalls = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 1,
            circuitMinimumThroughput: 2,
            circuitSamplingDuration: TimeSpan.FromTicks(1));

        timeProvider.Advance(TimeSpan.FromDays(3));
        using var first = await pipeline.SendAsync(
            (_, _) =>
            {
                factoryCalls++;
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/first"));
            },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        timeProvider.Advance(TimeSpan.FromDays(3));
        using var second = await pipeline.SendAsync(
            (_, _) =>
            {
                factoryCalls++;
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/second"));
            },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        using var third = await pipeline.SendAsync(
            (_, _) =>
            {
                factoryCalls++;
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/third"));
            },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        first.StatusCode.ShouldBe(HttpStatusCode.ServiceUnavailable);
        second.StatusCode.ShouldBe(HttpStatusCode.ServiceUnavailable);
        third.StatusCode.ShouldBe(HttpStatusCode.ServiceUnavailable);
        factoryCalls.ShouldBe(3);
    }

    [Fact]
    public async Task SendAsync_CircuitTransitionsHalfOpenAndClosedWithSafeTelemetry()
    {
        var timeProvider = new ManualTimeProvider();
        var events = new List<HttpCircuitTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 1,
            circuitMinimumThroughput: 2,
            onCircuit: (telemetry, _) => { events.Add(telemetry); return ValueTask.CompletedTask; });

        for (var i = 0; i < 2; i++)
        {
            using var response = await SendAsync(pipeline, (_, _, _) =>
                Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        }

        events.ShouldBe([
            new HttpCircuitTelemetry(
                "test",
                HttpResilienceCircuitState.Open,
                HttpStatusCode.ServiceUnavailable,
                HttpResilienceFailureCategory.HttpStatus),
        ]);

        timeProvider.Advance(TimeSpan.FromSeconds(5));
        using var final = await SendAsync(pipeline, (_, _, _) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));

        events.ShouldBe([
            new HttpCircuitTelemetry(
                "test",
                HttpResilienceCircuitState.Open,
                HttpStatusCode.ServiceUnavailable,
                HttpResilienceFailureCategory.HttpStatus),
            new HttpCircuitTelemetry(
                "test",
                HttpResilienceCircuitState.HalfOpen,
                null,
                HttpResilienceFailureCategory.CircuitOpen),
            new HttpCircuitTelemetry(
                "test",
                HttpResilienceCircuitState.Closed,
                HttpStatusCode.OK,
                HttpResilienceFailureCategory.HttpStatus),
        ]);
    }

    [Fact]
    public async Task SendAsync_TelemetryContainsNoRawExceptionBodyOrQueryValue()
    {
        const string secret = "never-emit-this-secret";
        var retryEvents = new List<HttpRetryTelemetry>();
        var circuitEvents = new List<HttpCircuitTelemetry>();
        var timeProvider = new ManualTimeProvider();
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 1,
            circuitMinimumThroughput: 2,
            onRetry: (telemetry, _) => { retryEvents.Add(telemetry); return ValueTask.CompletedTask; },
            onCircuit: (telemetry, _) => { circuitEvents.Add(telemetry); return ValueTask.CompletedTask; });

        for (var i = 0; i < 2; i++)
        {
            await Should.ThrowAsync<HttpRequestException>(() => pipeline.SendAsync(
                (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, $"https://example.test/path?token={secret}")
                {
                    Content = new StringContent(secret),
                }),
                (_, _, _) => Task.FromException<HttpResponseMessage>(new HttpRequestException(secret)),
                HttpCompletionOption.ResponseHeadersRead,
                HttpRequestReplaySafety.Replayable,
                cancellationToken: TestContext.Current.CancellationToken));
        }

        retryEvents.ShouldBeEmpty();
        var open = circuitEvents.Single();
        open.PipelineName.ShouldBe("test");
        open.State.ShouldBe(HttpResilienceCircuitState.Open);
        open.StatusCode.ShouldBeNull();
        open.FailureCategory.ShouldBe(HttpResilienceFailureCategory.Transport);
        open.ToString().ShouldNotContain(secret);
    }

    [Fact]
    public async Task SendAsync_ThrowingRetryCallbackDoesNotReplaceSuccessFinalFailureOrTimeout()
    {
        static ValueTask ThrowingRetry(HttpRetryTelemetry _, CancellationToken __) =>
            ValueTask.FromException(new InvalidOperationException("telemetry"));

        var successTime = new ManualTimeProvider();
        var successSends = 0;
        var successPipeline = CreatePipeline(successTime, maxAttempts: 2, onRetry: ThrowingRetry);
        var successOperation = SendAsync(successPipeline, (_, _, _) => Task.FromResult(
            ++successSends == 1
                ? new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                : new HttpResponseMessage(HttpStatusCode.OK)));
        await AdvanceRetryAsync(successOperation, successTime, TimeSpan.FromSeconds(1));
        using var success = await successOperation;
        success.StatusCode.ShouldBe(HttpStatusCode.OK);

        var failureTime = new ManualTimeProvider();
        var finalFailure = new HttpRequestException("final");
        var failureSends = 0;
        var failurePipeline = CreatePipeline(failureTime, maxAttempts: 2, onRetry: ThrowingRetry);
        var failureOperation = SendAsync(failurePipeline, (_, _, _) => Task.FromException<HttpResponseMessage>(
            ++failureSends == 1 ? new HttpRequestException("first") : finalFailure));
        await AdvanceRetryAsync(failureOperation, failureTime, TimeSpan.FromSeconds(1));
        (await Should.ThrowAsync<HttpRequestException>(() => failureOperation)).ShouldBeSameAs(finalFailure);

        var timeoutTime = new ManualTimeProvider();
        var timeoutPipeline = CreatePipeline(
            timeoutTime,
            maxAttempts: 2,
            totalTimeout: TimeSpan.FromMilliseconds(500),
            onRetry: ThrowingRetry);
        var timeoutOperation = SendAsync(timeoutPipeline, (_, _, _) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        await WaitForTimerCountAsync(timeoutTime, 2);
        timeoutTime.Advance(TimeSpan.FromMilliseconds(500));
        await Should.ThrowAsync<HttpRequestTimeoutException>(() => timeoutOperation);
    }

    [Fact]
    public async Task SendAsync_ThrowingCircuitCallbackDoesNotReplaceCallerCancellation()
    {
        static ValueTask ThrowingCircuit(HttpCircuitTelemetry _, CancellationToken __) =>
            ValueTask.FromException(new InvalidOperationException("telemetry"));

        var timeProvider = new ManualTimeProvider();
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 1,
            circuitMinimumThroughput: 2,
            onCircuit: ThrowingCircuit);

        for (var i = 0; i < 2; i++)
        {
            using var response = await SendAsync(pipeline, (_, _, _) =>
                Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        }

        timeProvider.Advance(TimeSpan.FromSeconds(5));
        using var cancellation = new CancellationTokenSource();
        var exception = await Should.ThrowAsync<OperationCanceledException>(() => SendAsyncWithCancellation(
            pipeline,
            (_, _, _) =>
            {
                cancellation.Cancel();
                return Task.FromException<HttpResponseMessage>(new OperationCanceledException(cancellation.Token));
            },
            cancellation.Token));

        exception.CancellationToken.ShouldBe(cancellation.Token);
    }

    private static HttpRequestResiliencePipeline CreatePipeline(
        ManualTimeProvider timeProvider,
        int maxAttempts = 3,
        TimeSpan? totalTimeout = null,
        TimeSpan? backoffDelay = null,
        TimeSpan? maximumDelay = null,
        int circuitMinimumThroughput = 20,
        TimeSpan? circuitSamplingDuration = null,
        TimeSpan? circuitBreakDuration = null,
        Func<double>? jitterProvider = null,
        IReadOnlySet<HttpStatusCode>? retryableStatusCodes = null,
        IReadOnlySet<HttpResilienceFailureCategory>? retryableExceptionCategories = null,
        Func<HttpRetryTelemetry, CancellationToken, ValueTask>? onRetry = null,
        Func<HttpTimeoutTelemetry, CancellationToken, ValueTask>? onTimeout = null,
        Func<HttpCircuitTelemetry, CancellationToken, ValueTask>? onCircuit = null)
        => new("test", new HttpRequestResilienceOptions
        {
            MaxAttempts = maxAttempts,
            TotalRequestTimeout = totalTimeout ?? TimeSpan.FromSeconds(30),
            BackoffBaseDelay = backoffDelay ?? TimeSpan.FromSeconds(1),
            MaximumDelay = maximumDelay ?? TimeSpan.FromSeconds(10),
            CircuitFailureRatio = 0.5,
            CircuitMinimumThroughput = circuitMinimumThroughput,
            CircuitSamplingDuration = circuitSamplingDuration ?? TimeSpan.FromSeconds(30),
            CircuitBreakDuration = circuitBreakDuration ?? TimeSpan.FromSeconds(5),
            TimeProvider = timeProvider,
            JitterProvider = jitterProvider ?? (() => 0),
            RetryableStatusCodes = retryableStatusCodes ?? new HttpRequestResilienceOptions().RetryableStatusCodes,
            RetryableExceptionCategories = retryableExceptionCategories ?? new HttpRequestResilienceOptions().RetryableExceptionCategories,
            OnRetry = onRetry,
            OnTimeout = onTimeout,
            OnCircuitStateChanged = onCircuit,
        });

    private static Task<HttpResponseMessage> SendAsync(
        HttpRequestResiliencePipeline pipeline,
        Func<HttpRequestMessage, HttpCompletionOption, CancellationToken, Task<HttpResponseMessage>> sender,
        HttpRequestReplaySafety replaySafety = HttpRequestReplaySafety.Replayable)
        => pipeline.SendAsync(
            (attempt, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, $"https://example.test/{attempt}")),
            sender,
            HttpCompletionOption.ResponseHeadersRead,
            replaySafety,
            cancellationToken: TestContext.Current.CancellationToken);

    private static Task<HttpResponseMessage> SendAsyncWithCancellation(
        HttpRequestResiliencePipeline pipeline,
        Func<HttpRequestMessage, HttpCompletionOption, CancellationToken, Task<HttpResponseMessage>> sender,
        CancellationToken cancellationToken)
        => pipeline.SendAsync(
            (attempt, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, $"https://example.test/{attempt}")),
            sender,
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: cancellationToken);

    private static async Task AdvanceRetryAsync(
        Task operation,
        ManualTimeProvider timeProvider,
        TimeSpan delay)
    {
        await WaitForTimerCountAsync(timeProvider, 2);
        timeProvider.Advance(delay);
        await Task.Yield();
    }

    private static async Task WaitForTimerCountAsync(ManualTimeProvider timeProvider, int count)
    {
        for (var i = 0; i < 10_000 && timeProvider.ActiveTimerCount < count; i++)
        {
            await Task.Yield();
        }

        timeProvider.ActiveTimerCount.ShouldBeGreaterThanOrEqualTo(count);
    }

    private sealed class TrackingContent : HttpContent
    {
        public bool Disposed { get; private set; }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) => Task.CompletedTask;

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return true;
        }

        protected override void Dispose(bool disposing)
        {
            Disposed = disposing;
            base.Dispose(disposing);
        }
    }

    private sealed class ThrowingDisposeContent(Exception exception) : HttpContent
    {
        public bool DisposeAttempted { get; private set; }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) => Task.CompletedTask;

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return true;
        }

        protected override void Dispose(bool disposing)
        {
            DisposeAttempted = disposing;
            base.Dispose(disposing);
            throw exception;
        }
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        private readonly object _gate = new();
        private readonly List<ManualTimer> _timers = [];
        private DateTimeOffset _utcNow;

        public ManualTimeProvider()
            : this(DateTimeOffset.UnixEpoch)
        {
        }

        public ManualTimeProvider(DateTimeOffset start) => _utcNow = start;

        public int ActiveTimerCount
        {
            get
            {
                lock (_gate)
                {
                    return _timers.Count(timer => timer.IsActive);
                }
            }
        }

        public int CreatedTimerCount { get; private set; }

        public override DateTimeOffset GetUtcNow()
        {
            lock (_gate)
            {
                return _utcNow;
            }
        }

        public override long GetTimestamp() => GetUtcNow().UtcTicks;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            ArgumentNullException.ThrowIfNull(callback);

            lock (_gate)
            {
                CreatedTimerCount++;
                var timer = new ManualTimer(this, callback, state, dueTime, period);
                _timers.Add(timer);
                return timer;
            }
        }

        public void Advance(TimeSpan duration)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(duration, TimeSpan.Zero);

            List<(TimerCallback Callback, object? State)> callbacks = [];
            lock (_gate)
            {
                _utcNow += duration;
                foreach (var timer in _timers)
                {
                    if (timer.TryFire(_utcNow, out var callback))
                    {
                        callbacks.Add(callback);
                    }
                }
            }

            foreach (var (callback, state) in callbacks)
            {
                callback(state);
            }
        }

        private sealed class ManualTimer : ITimer
        {
            private readonly ManualTimeProvider _owner;
            private readonly TimerCallback _callback;
            private readonly object? _state;
            private DateTimeOffset? _dueAt;
            private TimeSpan _period;

            public ManualTimer(
                ManualTimeProvider owner,
                TimerCallback callback,
                object? state,
                TimeSpan dueTime,
                TimeSpan period)
            {
                _owner = owner;
                _callback = callback;
                _state = state;
                ChangeCore(dueTime, period);
            }

            public bool IsActive => _dueAt is not null;

            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                lock (_owner._gate)
                {
                    ChangeCore(dueTime, period);
                    return true;
                }
            }

            public void Dispose()
            {
                lock (_owner._gate)
                {
                    _dueAt = null;
                }
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }

            public bool TryFire(
                DateTimeOffset utcNow,
                out (TimerCallback Callback, object? State) callback)
            {
                if (_dueAt is null || _dueAt > utcNow)
                {
                    callback = default;
                    return false;
                }

                callback = (_callback, _state);
                _dueAt = _period == Timeout.InfiniteTimeSpan ? null : utcNow + _period;
                return true;
            }

            private void ChangeCore(TimeSpan dueTime, TimeSpan period)
            {
                _period = period;
                _dueAt = dueTime == Timeout.InfiniteTimeSpan ? null : _owner._utcNow + dueTime;
            }
        }
    }
}
