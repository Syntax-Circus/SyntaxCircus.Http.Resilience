using System.Collections.Concurrent;
using System.Net.Http.Headers;

namespace SyntaxCircus.Http.Resilience.Tests;

public class HttpRequestResilienceFinalFixTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task SendAsync_PreCanceledOpenCallPreservesCallerTokenBeforeRequestConstruction()
    {
        var timeProvider = new ManualTimeProvider();
        var pipeline = CreatePipeline(timeProvider, circuitMinimumThroughput: 2);

        for (var i = 0; i < 2; i++)
        {
            using var failure = await SendAsync(
                pipeline,
                (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        }

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var factoryCalls = 0;

        var exception = await Should.ThrowAsync<OperationCanceledException>(() => pipeline.SendAsync(
            (_, _) =>
            {
                factoryCalls++;
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/canceled"));
            },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: cancellation.Token));

        exception.CancellationToken.ShouldBe(cancellation.Token);
        factoryCalls.ShouldBe(0);
    }

    [Fact]
    public async Task SendAsync_CanceledClosedCallContributesNoCircuitThroughput()
    {
        var timeProvider = new ManualTimeProvider();
        var pipeline = CreatePipeline(timeProvider, circuitMinimumThroughput: 2);
        using var cancellation = new CancellationTokenSource();

        var canceled = pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/canceled")),
            (_, _, _) =>
            {
                cancellation.Cancel();
                return Task.FromException<HttpResponseMessage>(new OperationCanceledException(cancellation.Token));
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: cancellation.Token);

        (await Should.ThrowAsync<OperationCanceledException>(() => canceled)).CancellationToken.ShouldBe(cancellation.Token);

        using var failure = await SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        var followingFactoryCalls = 0;
        using var following = await pipeline.SendAsync(
            (_, _) =>
            {
                followingFactoryCalls++;
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/following"));
            },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        followingFactoryCalls.ShouldBe(1);
        following.StatusCode.ShouldBe(HttpStatusCode.OK);
    }

    [Fact]
    public async Task SendAsync_CanceledHalfOpenProbeReleasesProbeWithoutClosingCircuit()
    {
        var timeProvider = new ManualTimeProvider();
        var events = new List<HttpCircuitTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            circuitMinimumThroughput: 2,
            onCircuit: (value, _) =>
            {
                events.Add(value);
                return ValueTask.CompletedTask;
            });

        await TripCircuitAsync(pipeline);
        timeProvider.Advance(TimeSpan.FromSeconds(5));

        using var cancellation = new CancellationTokenSource();
        var canceledProbe = SendAsyncWithCancellation(
            pipeline,
            (_, _, _) =>
            {
                cancellation.Cancel();
                return Task.FromException<HttpResponseMessage>(new OperationCanceledException(cancellation.Token));
            },
            cancellation.Token);
        (await Should.ThrowAsync<OperationCanceledException>(() => canceledProbe)).CancellationToken.ShouldBe(cancellation.Token);

        using var failedProbe = await SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));

        var factoryCalls = 0;
        await Should.ThrowAsync<HttpCircuitOpenException>(() => pipeline.SendAsync(
            (_, _) =>
            {
                factoryCalls++;
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/rejected"));
            },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken));

        factoryCalls.ShouldBe(0);
        events.Select(value => value.State).ShouldBe([
            HttpResilienceCircuitState.Open,
            HttpResilienceCircuitState.HalfOpen,
            HttpResilienceCircuitState.Open,
        ]);
    }

    [Fact]
    public async Task SendAsync_ObserverFailureHalfOpenProbeReleasesProbeWithoutClosingCircuit()
    {
        var timeProvider = new ManualTimeProvider();
        var pipeline = CreatePipeline(timeProvider, circuitMinimumThroughput: 2);
        await TripCircuitAsync(pipeline);
        timeProvider.Advance(TimeSpan.FromSeconds(5));
        var observerFailure = new InvalidOperationException("observer");

        var actual = await Record.ExceptionAsync(() => pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/observer")),
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            (_, _) => ValueTask.FromException(observerFailure),
            TestContext.Current.CancellationToken));

        actual.ShouldBeSameAs(observerFailure);
        using var failedProbe = await SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        await Should.ThrowAsync<HttpCircuitOpenException>(() => SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK))));
    }

    [Fact]
    public async Task SendAsync_LateCallerCanceledResponseIsObservedThenOwnedStateIsDisposedWithoutPolicyMutation()
    {
        var timeProvider = new ManualTimeProvider();
        var retryEvents = new List<HttpRetryTelemetry>();
        var circuitEvents = new List<HttpCircuitTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 2,
            circuitMinimumThroughput: 2,
            onRetry: (value, _) =>
            {
                retryEvents.Add(value);
                return ValueTask.CompletedTask;
            },
            onCircuit: (value, _) =>
            {
                circuitEvents.Add(value);
                return ValueTask.CompletedTask;
            });
        using var cancellation = new CancellationTokenSource();
        var senderEntered = NewSignal();
        var lateSender = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var requestContent = new TrackingContent();
        var responseContent = new TrackingContent();
        var observerCalls = 0;

        var operation = pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/late-response")
            {
                Content = requestContent,
            }),
            (_, _, _) =>
            {
                senderEntered.TrySetResult();
                return lateSender.Task;
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            (_, _) =>
            {
                Interlocked.Increment(ref observerCalls);
                return ValueTask.CompletedTask;
            },
            cancellation.Token);

        await senderEntered.Task.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken);
        cancellation.Cancel();
        var promptFailure = await Record.ExceptionAsync(() => operation.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken));
        requestContent.Disposed.ShouldBeFalse();

        lateSender.SetResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable) { Content = responseContent });
        await WaitForConditionAsync(() => requestContent.Disposed && responseContent.Disposed && Volatile.Read(ref observerCalls) == 1);
        _ = await Record.ExceptionAsync(() => operation);

        var cancellationFailure = promptFailure.ShouldBeOfType<OperationCanceledException>();
        cancellationFailure.CancellationToken.ShouldBe(cancellation.Token);
        retryEvents.ShouldBeEmpty();
        circuitEvents.ShouldBeEmpty();
    }

    [Fact]
    public async Task SendAsync_LateCallerCanceledTransportIsObservedAndRequestIsDisposedWithoutPolicyMutation()
    {
        var timeProvider = new ManualTimeProvider();
        var retryEvents = new List<HttpRetryTelemetry>();
        var circuitEvents = new List<HttpCircuitTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 2,
            circuitMinimumThroughput: 2,
            onRetry: (value, _) =>
            {
                retryEvents.Add(value);
                return ValueTask.CompletedTask;
            },
            onCircuit: (value, _) =>
            {
                circuitEvents.Add(value);
                return ValueTask.CompletedTask;
            });
        using var cancellation = new CancellationTokenSource();
        var senderEntered = NewSignal();
        var lateSender = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var requestContent = new TrackingContent();

        var operation = pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/late-transport")
            {
                Content = requestContent,
            }),
            (_, _, _) =>
            {
                senderEntered.TrySetResult();
                return lateSender.Task;
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: cancellation.Token);

        await senderEntered.Task.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken);
        cancellation.Cancel();
        var promptFailure = await Record.ExceptionAsync(() => operation.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken));
        requestContent.Disposed.ShouldBeFalse();

        lateSender.SetException(new HttpRequestException("late transport"));
        await WaitForConditionAsync(() => requestContent.Disposed);
        _ = await Record.ExceptionAsync(() => operation);

        var cancellationFailure = promptFailure.ShouldBeOfType<OperationCanceledException>();
        cancellationFailure.CancellationToken.ShouldBe(cancellation.Token);
        retryEvents.ShouldBeEmpty();
        circuitEvents.ShouldBeEmpty();
    }

    [Fact]
    public async Task SendAsync_LateCallerCanceledObserverIsObservedBeforeOwnedStateIsDisposed()
    {
        var pipeline = CreatePipeline(new ManualTimeProvider(), circuitMinimumThroughput: 2);
        using var cancellation = new CancellationTokenSource();
        var observerEntered = NewSignal();
        var lateObserver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var requestContent = new TrackingContent();
        var responseContent = new TrackingContent();

        var operation = pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/late-observer")
            {
                Content = requestContent,
            }),
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = responseContent }),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            (_, _) =>
            {
                observerEntered.TrySetResult();
                return new ValueTask(lateObserver.Task);
            },
            cancellation.Token);

        await observerEntered.Task.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken);
        cancellation.Cancel();
        var promptFailure = await Record.ExceptionAsync(() => operation.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken));
        requestContent.Disposed.ShouldBeFalse();
        responseContent.Disposed.ShouldBeFalse();

        lateObserver.SetException(new InvalidOperationException("late observer"));
        await WaitForConditionAsync(() => requestContent.Disposed && responseContent.Disposed);
        _ = await Record.ExceptionAsync(() => operation);

        var cancellationFailure = promptFailure.ShouldBeOfType<OperationCanceledException>();
        cancellationFailure.CancellationToken.ShouldBe(cancellation.Token);
    }

    [Fact]
    public async Task SendAsync_LateSuccessBeyondBudgetTimesOutPromptlyCountsOnceAndCannotCloseCircuit()
    {
        var timeProvider = new ManualTimeProvider();
        var timeoutEvents = new List<HttpTimeoutTelemetry>();
        var circuitEvents = new List<HttpCircuitTelemetry>();
        var observerCalls = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            totalTimeout: TimeSpan.FromSeconds(1),
            circuitMinimumThroughput: 2,
            circuitFailureRatio: 1,
            onTimeout: (value, _) =>
            {
                timeoutEvents.Add(value);
                return ValueTask.CompletedTask;
            },
            onCircuit: (value, _) =>
            {
                circuitEvents.Add(value);
                return ValueTask.CompletedTask;
            });

        for (var i = 0; i < 2; i++)
        {
            var senderEntered = NewSignal();
            var lateSender = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            var requestContent = new TrackingContent();
            var responseContent = new TrackingContent();
            var operation = pipeline.SendAsync(
                (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, $"https://example.test/late-success/{i}")
                {
                    Content = requestContent,
                }),
                (_, _, _) =>
                {
                    senderEntered.TrySetResult();
                    return lateSender.Task;
                },
                HttpCompletionOption.ResponseHeadersRead,
                HttpRequestReplaySafety.Replayable,
                (_, _) =>
                {
                    Interlocked.Increment(ref observerCalls);
                    return ValueTask.CompletedTask;
                },
                TestContext.Current.CancellationToken);

            await senderEntered.Task.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken);
            timeProvider.Advance(TimeSpan.FromSeconds(1));
            var promptFailure = await Record.ExceptionAsync(() => operation.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken));
            lateSender.SetResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = responseContent });
            var disposedByPipeline = await TryWaitForConditionAsync(() => requestContent.Disposed && responseContent.Disposed);
            await ObserveAndDisposeResponseAsync(operation);
            promptFailure.ShouldBeOfType<HttpRequestTimeoutException>();
            disposedByPipeline.ShouldBeTrue();
        }

        var factoryCalls = 0;
        await Should.ThrowAsync<HttpCircuitOpenException>(() => pipeline.SendAsync(
            (_, _) =>
            {
                factoryCalls++;
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/rejected"));
            },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken));

        factoryCalls.ShouldBe(0);
        Volatile.Read(ref observerCalls).ShouldBe(2);
        timeoutEvents.Count.ShouldBe(2);
        circuitEvents.Count(value => value.State == HttpResilienceCircuitState.Open).ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_LateTransportBeyondBudgetIsObservedDisposedAndCannotMutateCircuitAgain()
    {
        var timeProvider = new ManualTimeProvider();
        var timeoutEvents = new List<HttpTimeoutTelemetry>();
        var circuitEvents = new List<HttpCircuitTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            totalTimeout: TimeSpan.FromSeconds(1),
            circuitMinimumThroughput: 2,
            circuitFailureRatio: 1,
            onTimeout: (value, _) =>
            {
                timeoutEvents.Add(value);
                return ValueTask.CompletedTask;
            },
            onCircuit: (value, _) =>
            {
                circuitEvents.Add(value);
                return ValueTask.CompletedTask;
            });
        var senderEntered = NewSignal();
        var lateSender = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var requestContent = new TrackingContent();
        var operation = pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/late-transport-timeout")
            {
                Content = requestContent,
            }),
            (_, _, _) =>
            {
                senderEntered.TrySetResult();
                return lateSender.Task;
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        await senderEntered.Task.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken);
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        var promptFailure = await Record.ExceptionAsync(() => operation.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken));
        lateSender.SetException(new HttpRequestException("late transport"));
        await WaitForConditionAsync(() => requestContent.Disposed);
        _ = await Record.ExceptionAsync(() => operation);

        promptFailure.ShouldBeOfType<HttpRequestTimeoutException>();
        timeoutEvents.Count.ShouldBe(1);
        circuitEvents.ShouldBeEmpty();
        using var following = await SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));
        following.StatusCode.ShouldBe(HttpStatusCode.OK);
        circuitEvents.ShouldBeEmpty();
    }

    [Fact]
    public async Task SendAsync_LateObserverBeyondBudgetIsObservedBeforeOwnedStateIsDisposedAndCannotMutateCircuitAgain()
    {
        var timeProvider = new ManualTimeProvider();
        var timeoutEvents = new List<HttpTimeoutTelemetry>();
        var circuitEvents = new List<HttpCircuitTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            totalTimeout: TimeSpan.FromSeconds(1),
            circuitMinimumThroughput: 2,
            circuitFailureRatio: 1,
            onTimeout: (value, _) =>
            {
                timeoutEvents.Add(value);
                return ValueTask.CompletedTask;
            },
            onCircuit: (value, _) =>
            {
                circuitEvents.Add(value);
                return ValueTask.CompletedTask;
            });
        var observerEntered = NewSignal();
        var lateObserver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var requestContent = new TrackingContent();
        var responseContent = new TrackingContent();
        var operation = pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/late-observer-timeout")
            {
                Content = requestContent,
            }),
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = responseContent }),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            (_, _) =>
            {
                observerEntered.TrySetResult();
                return new ValueTask(lateObserver.Task);
            },
            TestContext.Current.CancellationToken);

        await observerEntered.Task.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken);
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        var promptFailure = await Record.ExceptionAsync(() => operation.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken));
        requestContent.Disposed.ShouldBeFalse();
        responseContent.Disposed.ShouldBeFalse();
        lateObserver.SetException(new InvalidOperationException("late observer"));
        await WaitForConditionAsync(() => requestContent.Disposed && responseContent.Disposed);
        _ = await Record.ExceptionAsync(() => operation);

        promptFailure.ShouldBeOfType<HttpRequestTimeoutException>();
        timeoutEvents.Count.ShouldBe(1);
        circuitEvents.ShouldBeEmpty();
        using var following = await SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));
        following.StatusCode.ShouldBe(HttpStatusCode.OK);
        circuitEvents.ShouldBeEmpty();
    }

    [Fact]
    public async Task SendAsync_NeverCompletingSenderReturnsLogicalTimeoutPromptlyWithoutPrematureRequestDisposal()
    {
        var timeProvider = new ManualTimeProvider();
        var pipeline = CreatePipeline(timeProvider, totalTimeout: TimeSpan.FromSeconds(1));
        var senderEntered = NewSignal();
        var lateSender = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var requestContent = new TrackingContent();
        var responseContent = new TrackingContent();
        var operation = pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/never")
            {
                Content = requestContent,
            }),
            (_, _, _) =>
            {
                senderEntered.TrySetResult();
                return lateSender.Task;
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        await senderEntered.Task.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken);
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        var promptFailure = await Record.ExceptionAsync(() => operation.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken));
        requestContent.Disposed.ShouldBeFalse();

        lateSender.SetResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = responseContent });
        var disposedByPipeline = await TryWaitForConditionAsync(() => requestContent.Disposed && responseContent.Disposed);
        await ObserveAndDisposeResponseAsync(operation);

        promptFailure.ShouldBeOfType<HttpRequestTimeoutException>();
        disposedByPipeline.ShouldBeTrue();
    }

    [Fact]
    public async Task SendAsync_LateFactoryResultBeyondBudgetIsDisposedAndSenderNeverStarts()
    {
        var timeProvider = new ManualTimeProvider();
        var pipeline = CreatePipeline(timeProvider, totalTimeout: TimeSpan.FromSeconds(1));
        var factoryEntered = NewSignal();
        var lateFactory = new TaskCompletionSource<HttpRequestMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var requestContent = new TrackingContent();
        var senderCalls = 0;
        var operation = pipeline.SendAsync(
            (_, _) =>
            {
                factoryEntered.TrySetResult();
                return new ValueTask<HttpRequestMessage>(lateFactory.Task);
            },
            (_, _, _) =>
            {
                senderCalls++;
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        await factoryEntered.Task.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken);
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        var promptFailure = await Record.ExceptionAsync(() => operation.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken));
        lateFactory.SetResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/late-factory")
        {
            Content = requestContent,
        });
        await WaitForConditionAsync(() => requestContent.Disposed);
        _ = await Record.ExceptionAsync(() => operation);

        promptFailure.ShouldBeOfType<HttpRequestTimeoutException>();
        senderCalls.ShouldBe(0);
    }

    [Fact]
    public async Task SendAsync_ElapsedBudgetAtRetryBoundaryNeverStartsZeroDelayAttempt()
    {
        var timeProvider = new ManualTimeProvider();
        var sends = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 2,
            totalTimeout: TimeSpan.FromSeconds(1),
            backoffDelay: TimeSpan.FromMilliseconds(100),
            maximumDelay: TimeSpan.FromSeconds(1));

        var exception = await Should.ThrowAsync<HttpRequestTimeoutException>(() => SendAsync(
            pipeline,
            (_, _, _) =>
            {
                sends++;
                timeProvider.AdvanceWithoutFiringTimers(TimeSpan.FromSeconds(1));
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
            }));

        exception.Timeout.ShouldBe(TimeSpan.FromSeconds(1));
        sends.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_LongFiniteBudgetConstructsAndSendsWithoutPlatformTimerRangeFailure()
    {
        var pipeline = new HttpRequestResiliencePipeline("long-budget", new HttpRequestResilienceOptions
        {
            MaxAttempts = 1,
            TotalRequestTimeout = TimeSpan.FromDays(60),
            CircuitMinimumThroughput = 20,
            TimeProvider = TimeProvider.System,
        });
        var sends = 0;

        using var response = await pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/long")),
            (_, _, _) =>
            {
                sends++;
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        sends.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_SegmentedSubQuantumAdvancesPruneCircuitWindowAtExactConfiguredDuration()
    {
        var timeProvider = new ManualTimeProvider();
        var events = new List<HttpCircuitTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            circuitMinimumThroughput: 2,
            circuitFailureRatio: 0.6,
            circuitSamplingDuration: TimeSpan.FromDays(2) + TimeSpan.FromTicks(1),
            onCircuit: (value, _) =>
            {
                events.Add(value);
                return ValueTask.CompletedTask;
            });

        using var firstFailure = await SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        timeProvider.Advance(TimeSpan.FromDays(1));
        using var midpointSuccess = await SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));
        timeProvider.Advance(TimeSpan.FromDays(1) + TimeSpan.FromTicks(1));
        using var boundaryFailure = await SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        var finalFactoryCalls = 0;
        using var final = await pipeline.SendAsync(
            (_, _) =>
            {
                finalFactoryCalls++;
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/final"));
            },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        finalFactoryCalls.ShouldBe(1);
        final.StatusCode.ShouldBe(HttpStatusCode.OK);
        events.ShouldBeEmpty();
    }

    [Fact]
    public async Task SendAsync_ZeroOriginCircuitTimestampReturnsExactRetryAfter()
    {
        var timeProvider = new ManualTimeProvider();
        timeProvider.GetTimestamp().ShouldBe(0);
        var pipeline = CreatePipeline(timeProvider, circuitMinimumThroughput: 2);
        await TripCircuitAsync(pipeline);

        var exception = await Should.ThrowAsync<HttpCircuitOpenException>(() => SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK))));

        exception.RetryAfter.ShouldBe(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task SendAsync_MaximumSamplingAndBreakDurationsRemainExact()
    {
        var timeProvider = new ManualTimeProvider();
        var pipeline = CreatePipeline(
            timeProvider,
            circuitMinimumThroughput: 2,
            circuitFailureRatio: 1,
            circuitSamplingDuration: TimeSpan.MaxValue,
            circuitBreakDuration: TimeSpan.MaxValue);

        using var first = await SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        timeProvider.Advance(TimeSpan.FromDays(3));
        using var second = await SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));

        var initial = await Should.ThrowAsync<HttpCircuitOpenException>(() => SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK))));
        initial.RetryAfter.ShouldBe(TimeSpan.MaxValue);

        timeProvider.Advance(TimeSpan.FromDays(3));
        var later = await Should.ThrowAsync<HttpCircuitOpenException>(() => SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK))));
        later.RetryAfter.ShouldBe(TimeSpan.MaxValue - TimeSpan.FromDays(3));
    }

    [Fact]
    public async Task SendAsync_ConcurrentTripRejectAndHalfOpenAdmitExactlyOneProbe()
    {
        var timeProvider = new ManualTimeProvider();
        var events = new ConcurrentQueue<HttpCircuitTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            circuitMinimumThroughput: 2,
            circuitFailureRatio: 1,
            onCircuit: (value, _) =>
            {
                events.Enqueue(value);
                return ValueTask.CompletedTask;
            });
        var failuresEntered = 0;
        var failuresReady = NewSignal();
        var releaseFailures = NewSignal();

        Task<HttpResponseMessage> ConcurrentFailure() => SendAsync(
            pipeline,
            async (_, _, _) =>
            {
                if (Interlocked.Increment(ref failuresEntered) == 2)
                {
                    failuresReady.TrySetResult();
                }

                await releaseFailures.Task;
                return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
            });

        var first = ConcurrentFailure();
        var second = ConcurrentFailure();
        await failuresReady.Task.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken);
        releaseFailures.TrySetResult();
        using (await first)
        using (await second)
        {
        }

        var rejectedFactoryCalls = 0;
        await Should.ThrowAsync<HttpCircuitOpenException>(() => pipeline.SendAsync(
            (_, _) =>
            {
                rejectedFactoryCalls++;
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/open"));
            },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken));
        rejectedFactoryCalls.ShouldBe(0);

        timeProvider.Advance(TimeSpan.FromSeconds(5));
        var probeEntered = NewSignal();
        var releaseProbe = NewSignal();
        var probeSends = 0;
        var probe = SendAsync(
            pipeline,
            async (_, _, _) =>
            {
                Interlocked.Increment(ref probeSends);
                probeEntered.TrySetResult();
                await releaseProbe.Task;
                return new HttpResponseMessage(HttpStatusCode.OK);
            });
        await probeEntered.Task.WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken);

        var contenderFactoryCalls = 0;
        var contenders = Enumerable.Range(0, 8)
            .Select(_ => pipeline.SendAsync(
                (_, _) =>
                {
                    Interlocked.Increment(ref contenderFactoryCalls);
                    return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/contender"));
                },
                (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
                HttpCompletionOption.ResponseHeadersRead,
                HttpRequestReplaySafety.Replayable,
                cancellationToken: TestContext.Current.CancellationToken))
            .Select(CaptureExceptionAsync)
            .ToArray();
        var contenderFailures = await Task.WhenAll(contenders).WaitAsync(SafetyTimeout, TestContext.Current.CancellationToken);
        releaseProbe.TrySetResult();
        using var probeResponse = await probe;

        probeSends.ShouldBe(1);
        contenderFactoryCalls.ShouldBe(0);
        contenderFailures.ShouldAllBe(value => value is HttpCircuitOpenException);
        events.Select(value => value.State).ShouldBe([
            HttpResilienceCircuitState.Open,
            HttpResilienceCircuitState.HalfOpen,
            HttpResilienceCircuitState.Closed,
        ]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SendAsync_InvalidOrPastRetryAfterFallsBackToDeterministicBackoff(bool pastDate)
    {
        var timeProvider = new ManualTimeProvider();
        var retries = new List<HttpRetryTelemetry>();
        var sends = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 2,
            jitterProvider: () => 0.25,
            onRetry: (value, _) =>
            {
                retries.Add(value);
                return ValueTask.CompletedTask;
            });
        var operation = SendAsync(pipeline, (_, _, _) =>
        {
            if (++sends > 1)
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            }

            var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
            if (pastDate)
            {
                response.Headers.RetryAfter = new RetryConditionHeaderValue(timeProvider.GetUtcNow().AddSeconds(-1));
            }
            else
            {
                response.Headers.TryAddWithoutValidation("Retry-After", "not-a-delay");
            }

            return Task.FromResult(response);
        });

        await WaitForConditionAsync(() => retries.Count == 1 && timeProvider.ActiveTimerCount >= 2);
        retries[0].Delay.ShouldBe(TimeSpan.FromMilliseconds(1250));
        timeProvider.Advance(retries[0].Delay);
        using var response = await operation;

        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        sends.ShouldBe(2);
    }

    [Fact]
    public async Task SendAsync_ExponentialJitterSequenceIsDeterministic()
    {
        var timeProvider = new ManualTimeProvider();
        var retries = new List<HttpRetryTelemetry>();
        var sends = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 4,
            jitterProvider: () => 0.25,
            onRetry: (value, _) =>
            {
                retries.Add(value);
                return ValueTask.CompletedTask;
            });
        var operation = SendAsync(pipeline, (_, _, _) => Task.FromResult(new HttpResponseMessage(
            ++sends <= 3 ? HttpStatusCode.ServiceUnavailable : HttpStatusCode.OK)));
        var expected = new[]
        {
            TimeSpan.FromMilliseconds(1250),
            TimeSpan.FromMilliseconds(2500),
            TimeSpan.FromSeconds(5),
        };

        for (var i = 0; i < expected.Length; i++)
        {
            var retryIndex = i;
            await WaitForConditionAsync(() => retries.Count > retryIndex && timeProvider.ActiveTimerCount >= 2);
            retries[retryIndex].Delay.ShouldBe(expected[retryIndex]);
            timeProvider.Advance(expected[retryIndex]);
        }

        using var response = await operation;
        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        retries.Select(value => value.Delay).ShouldBe(expected);
    }

    [Fact]
    public async Task SendAsync_BackoffSequenceClampsEveryDelayToMaximum()
    {
        var timeProvider = new ManualTimeProvider();
        var retries = new List<HttpRetryTelemetry>();
        var sends = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 3,
            backoffDelay: TimeSpan.FromSeconds(2),
            maximumDelay: TimeSpan.FromSeconds(3),
            jitterProvider: () => 0.5,
            onRetry: (value, _) =>
            {
                retries.Add(value);
                return ValueTask.CompletedTask;
            });
        var operation = SendAsync(pipeline, (_, _, _) => Task.FromResult(new HttpResponseMessage(
            ++sends <= 2 ? HttpStatusCode.ServiceUnavailable : HttpStatusCode.OK)));

        for (var i = 0; i < 2; i++)
        {
            var retryIndex = i;
            await WaitForConditionAsync(() => retries.Count > retryIndex && timeProvider.ActiveTimerCount >= 2);
            retries[retryIndex].Delay.ShouldBe(TimeSpan.FromSeconds(3));
            timeProvider.Advance(TimeSpan.FromSeconds(3));
        }

        using var response = await operation;
        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        retries.Select(value => value.Delay).ShouldBe([
            TimeSpan.FromSeconds(3),
            TimeSpan.FromSeconds(3),
        ]);
    }

    [Fact]
    public async Task SendAsync_NonCallerHandlerTimeoutRetriesAndReportsTimeoutCategory()
    {
        var timeProvider = new ManualTimeProvider();
        var retries = new List<HttpRetryTelemetry>();
        var sends = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 2,
            onRetry: (value, _) =>
            {
                retries.Add(value);
                return ValueTask.CompletedTask;
            });
        using var handlerTimeout = new CancellationTokenSource();
        handlerTimeout.Cancel();
        var operation = SendAsync(pipeline, (_, _, _) => ++sends == 1
            ? Task.FromException<HttpResponseMessage>(new OperationCanceledException("handler timeout", handlerTimeout.Token))
            : Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));

        await WaitForConditionAsync(() => retries.Count == 1 && timeProvider.ActiveTimerCount >= 2);
        retries[0].FailureCategory.ShouldBe(HttpResilienceFailureCategory.Timeout);
        timeProvider.Advance(retries[0].Delay);
        using var response = await operation;

        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        sends.ShouldBe(2);
    }

    private static HttpRequestResiliencePipeline CreatePipeline(
        ManualTimeProvider timeProvider,
        int maxAttempts = 1,
        TimeSpan? totalTimeout = null,
        TimeSpan? backoffDelay = null,
        TimeSpan? maximumDelay = null,
        double circuitFailureRatio = 0.5,
        int circuitMinimumThroughput = 20,
        TimeSpan? circuitSamplingDuration = null,
        TimeSpan? circuitBreakDuration = null,
        Func<double>? jitterProvider = null,
        Func<HttpRetryTelemetry, CancellationToken, ValueTask>? onRetry = null,
        Func<HttpTimeoutTelemetry, CancellationToken, ValueTask>? onTimeout = null,
        Func<HttpCircuitTelemetry, CancellationToken, ValueTask>? onCircuit = null)
        => new("test", new HttpRequestResilienceOptions
        {
            MaxAttempts = maxAttempts,
            TotalRequestTimeout = totalTimeout ?? TimeSpan.FromSeconds(30),
            BackoffBaseDelay = backoffDelay ?? TimeSpan.FromSeconds(1),
            MaximumDelay = maximumDelay ?? TimeSpan.FromSeconds(10),
            CircuitFailureRatio = circuitFailureRatio,
            CircuitMinimumThroughput = circuitMinimumThroughput,
            CircuitSamplingDuration = circuitSamplingDuration ?? TimeSpan.FromSeconds(30),
            CircuitBreakDuration = circuitBreakDuration ?? TimeSpan.FromSeconds(5),
            TimeProvider = timeProvider,
            JitterProvider = jitterProvider ?? (() => 0),
            OnRetry = onRetry,
            OnTimeout = onTimeout,
            OnCircuitStateChanged = onCircuit,
        });

    private static Task<HttpResponseMessage> SendAsync(
        HttpRequestResiliencePipeline pipeline,
        Func<HttpRequestMessage, HttpCompletionOption, CancellationToken, Task<HttpResponseMessage>> sender)
        => pipeline.SendAsync(
            (attempt, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, $"https://example.test/{attempt}")),
            sender,
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
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

    private static async Task TripCircuitAsync(HttpRequestResiliencePipeline pipeline)
    {
        for (var i = 0; i < 2; i++)
        {
            using var failure = await SendAsync(
                pipeline,
                (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        }
    }

    private static async Task<Exception?> CaptureExceptionAsync(Task<HttpResponseMessage> operation)
    {
        try
        {
            using var response = await operation;
            return null;
        }
        catch (Exception exception)
        {
            return exception;
        }
    }

    private static async Task ObserveAndDisposeResponseAsync(Task<HttpResponseMessage> operation)
    {
        try
        {
            using var response = await operation;
        }
        catch
        {
        }
    }

    private static TaskCompletionSource NewSignal()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static async Task WaitForConditionAsync(Func<bool> condition)
    {
        (await TryWaitForConditionAsync(condition)).ShouldBeTrue();
    }

    private static async Task<bool> TryWaitForConditionAsync(Func<bool> condition)
    {
        var start = TimeProvider.System.GetTimestamp();
        while (!condition() && TimeProvider.System.GetElapsedTime(start) < SafetyTimeout)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(1), TestContext.Current.CancellationToken);
        }

        return condition();
    }

    private sealed class TrackingContent : HttpContent
    {
        private int _disposed;

        public bool Disposed => Volatile.Read(ref _disposed) != 0;

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) => Task.CompletedTask;

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return true;
        }

        protected override void Dispose(bool disposing)
        {
            Interlocked.Exchange(ref _disposed, 1);
            base.Dispose(disposing);
        }
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        private readonly object _gate = new();
        private readonly List<ManualTimer> _timers = [];
        private long _timestamp;
        private DateTimeOffset _utcNow = DateTimeOffset.UnixEpoch;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public int ActiveTimerCount
        {
            get
            {
                lock (_gate)
                {
                    return _timers.Count(value => value.IsActive);
                }
            }
        }

        public override long GetTimestamp()
        {
            lock (_gate)
            {
                return _timestamp;
            }
        }

        public override DateTimeOffset GetUtcNow()
        {
            lock (_gate)
            {
                return _utcNow;
            }
        }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            ArgumentNullException.ThrowIfNull(callback);
            lock (_gate)
            {
                var timer = new ManualTimer(this, callback, state, dueTime, period);
                _timers.Add(timer);
                return timer;
            }
        }

        public void Advance(TimeSpan duration)
        {
            List<(TimerCallback Callback, object? State)> callbacks = [];
            lock (_gate)
            {
                AdvanceClockCore(duration);
                foreach (var timer in _timers)
                {
                    if (timer.TryFire(_timestamp, out var callback))
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

        public void AdvanceWithoutFiringTimers(TimeSpan duration)
        {
            lock (_gate)
            {
                AdvanceClockCore(duration);
            }
        }

        private void AdvanceClockCore(TimeSpan duration)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(duration, TimeSpan.Zero);
            _timestamp = checked(_timestamp + duration.Ticks);
            _utcNow += duration;
        }

        private sealed class ManualTimer : ITimer
        {
            private readonly ManualTimeProvider _owner;
            private readonly TimerCallback _callback;
            private readonly object? _state;
            private long? _dueTimestamp;
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

            public bool IsActive => _dueTimestamp is not null;

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
                    _dueTimestamp = null;
                }
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }

            public bool TryFire(long timestamp, out (TimerCallback Callback, object? State) callback)
            {
                if (_dueTimestamp is null || _dueTimestamp > timestamp)
                {
                    callback = default;
                    return false;
                }

                callback = (_callback, _state);
                _dueTimestamp = _period == Timeout.InfiniteTimeSpan
                    ? null
                    : SaturatingAdd(timestamp, _period.Ticks);
                return true;
            }

            private void ChangeCore(TimeSpan dueTime, TimeSpan period)
            {
                _period = period;
                _dueTimestamp = dueTime == Timeout.InfiniteTimeSpan
                    ? null
                    : SaturatingAdd(_owner._timestamp, dueTime.Ticks);
            }

            private static long SaturatingAdd(long value, long increment)
            {
                var result = (System.Numerics.BigInteger)value + increment;
                return result >= long.MaxValue ? long.MaxValue : (long)result;
            }
        }
    }
}
