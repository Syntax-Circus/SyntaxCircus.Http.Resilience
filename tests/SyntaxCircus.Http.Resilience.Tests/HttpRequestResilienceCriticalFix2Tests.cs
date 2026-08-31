using System.Collections.Concurrent;

namespace SyntaxCircus.Http.Resilience.Tests;

public class HttpRequestResilienceCriticalFix2Tests
{
    private static readonly TimeSpan PromptSafetyTimeout = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task SendAsync_CancellationObservedDuringCompletionTimestampWinsWithoutClosedCircuitThroughput()
    {
        var timeProvider = new CallbackTimeProvider();
        var pipeline = CreatePipeline(
            timeProvider,
            circuitFailureRatio: 0.5,
            circuitMinimumThroughput: 2);
        using var cancellation = new CancellationTokenSource();
        var senderEntered = NewSignal();
        var senderCompletion = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var canceledOperation = pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/canceled")),
            (_, _, _) =>
            {
                senderEntered.TrySetResult();
                return senderCompletion.Task;
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: cancellation.Token);

        await senderEntered.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        timeProvider.InvokeOnNextTimestamp(cancellation.Cancel);
        senderCompletion.SetResult(new HttpResponseMessage(HttpStatusCode.OK));

        var canceled = await Should.ThrowAsync<OperationCanceledException>(() => canceledOperation);
        canceled.CancellationToken.ShouldBe(cancellation.Token);

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

        failure.StatusCode.ShouldBe(HttpStatusCode.ServiceUnavailable);
        followingFactoryCalls.ShouldBe(1);
        following.StatusCode.ShouldBe(HttpStatusCode.OK);
    }

    [Fact]
    public async Task SendAsync_CancellationObservedDuringHalfOpenFailureTimestampReleasesProbeWithoutReopening()
    {
        var timeProvider = new CallbackTimeProvider();
        var pipeline = CreatePipeline(
            timeProvider,
            circuitFailureRatio: 1,
            circuitMinimumThroughput: 2);
        await TripCircuitAsync(pipeline);
        timeProvider.Advance(TimeSpan.FromSeconds(5));

        using var cancellation = new CancellationTokenSource();
        var senderEntered = NewSignal();
        var senderCompletion = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var canceledProbe = pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/canceled-probe")),
            (_, _, _) =>
            {
                senderEntered.TrySetResult();
                return senderCompletion.Task;
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: cancellation.Token);

        await senderEntered.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        timeProvider.InvokeOnNextTimestamp(cancellation.Cancel);
        senderCompletion.SetResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));

        var canceled = await Should.ThrowAsync<OperationCanceledException>(() => canceledProbe);
        canceled.CancellationToken.ShouldBe(cancellation.Token);

        var followingFactoryCalls = 0;
        using var followingProbe = await pipeline.SendAsync(
            (_, _) =>
            {
                followingFactoryCalls++;
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/following-probe"));
            },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken);

        followingFactoryCalls.ShouldBe(1);
        followingProbe.StatusCode.ShouldBe(HttpStatusCode.OK);
    }

    [Fact]
    public async Task SendAsync_CancellationInitiatedByPostCommitCircuitCallbackCannotReplaceCommittedOutcome()
    {
        var timeProvider = new CallbackTimeProvider();
        CancellationTokenSource? triggeringCancellation = null;
        var circuitEvents = new List<HttpCircuitTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            circuitFailureRatio: 1,
            circuitMinimumThroughput: 2,
            onCircuit: (value, _) =>
            {
                circuitEvents.Add(value);
                if (value.State == HttpResilienceCircuitState.Open)
                {
                    triggeringCancellation?.Cancel();
                }

                return ValueTask.CompletedTask;
            });

        using (var primer = await SendAsync(
            pipeline,
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable))))
        {
            primer.StatusCode.ShouldBe(HttpStatusCode.ServiceUnavailable);
        }

        using var cancellation = new CancellationTokenSource();
        triggeringCancellation = cancellation;
        using var triggeringResponse = await pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/trigger")),
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: cancellation.Token);

        triggeringResponse.StatusCode.ShouldBe(HttpStatusCode.ServiceUnavailable);
        cancellation.IsCancellationRequested.ShouldBeTrue();
        circuitEvents.Select(value => value.State).ShouldBe([HttpResilienceCircuitState.Open]);

        var rejectedFactoryCalls = 0;
        await Should.ThrowAsync<HttpCircuitOpenException>(() => pipeline.SendAsync(
            (_, _) =>
            {
                rejectedFactoryCalls++;
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/rejected"));
            },
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken));
        rejectedFactoryCalls.ShouldBe(0);
    }

    [Fact]
    public async Task SendAsync_SynchronouslyBlockingFactoryIsRacedByHardDeadline()
    {
        var timeProvider = new CallbackTimeProvider();
        var timeoutEvents = new ConcurrentQueue<HttpTimeoutTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            totalTimeout: TimeSpan.FromSeconds(1),
            onTimeout: (value, _) =>
            {
                timeoutEvents.Enqueue(value);
                return ValueTask.CompletedTask;
            });
        using var releaseFactory = new ManualResetEventSlim();
        var factoryEntered = NewSignal();
        var requestContent = new TrackingContent();
        var senderCalls = 0;
        var operation = StartPipelineCall(() => pipeline.SendAsync(
            (_, _) =>
            {
                factoryEntered.TrySetResult();
                releaseFactory.Wait(CancellationToken.None);
                return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/factory")
                {
                    Content = requestContent,
                });
            },
            (_, _, _) =>
            {
                Interlocked.Increment(ref senderCalls);
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            cancellationToken: TestContext.Current.CancellationToken));

        await factoryEntered.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        var promptFailure = await CaptureBeforeReleaseAsync(operation, releaseFactory);
        await WaitForConditionAsync(() => requestContent.Disposed);
        await ObserveAndDisposeResponseAsync(operation);

        promptFailure.ShouldBeOfType<HttpRequestTimeoutException>();
        senderCalls.ShouldBe(0);
        timeoutEvents.Count.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_SynchronouslyBlockingSenderIsRacedByHardDeadlineAndLateResponseIsObservedOnce()
    {
        var timeProvider = new CallbackTimeProvider();
        var timeoutEvents = new ConcurrentQueue<HttpTimeoutTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            totalTimeout: TimeSpan.FromSeconds(1),
            onTimeout: (value, _) =>
            {
                timeoutEvents.Enqueue(value);
                return ValueTask.CompletedTask;
            });
        using var releaseSender = new ManualResetEventSlim();
        var senderEntered = NewSignal();
        var requestContent = new TrackingContent();
        var responseContent = new TrackingContent();
        var observerCalls = 0;
        var operation = StartPipelineCall(() => pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/sender")
            {
                Content = requestContent,
            }),
            (_, _, _) =>
            {
                senderEntered.TrySetResult();
                releaseSender.Wait(CancellationToken.None);
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = responseContent,
                });
            },
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            (_, _) =>
            {
                Interlocked.Increment(ref observerCalls);
                return ValueTask.CompletedTask;
            },
            TestContext.Current.CancellationToken));

        await senderEntered.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        var promptFailure = await CaptureBeforeReleaseAsync(
            operation,
            releaseSender,
            () =>
            {
                requestContent.Disposed.ShouldBeFalse();
                responseContent.Disposed.ShouldBeFalse();
            });
        await WaitForConditionAsync(() =>
            requestContent.Disposed
            && responseContent.Disposed
            && Volatile.Read(ref observerCalls) == 1);
        await ObserveAndDisposeResponseAsync(operation);

        promptFailure.ShouldBeOfType<HttpRequestTimeoutException>();
        timeoutEvents.Count.ShouldBe(1);
        observerCalls.ShouldBe(1);
    }

    [Fact]
    public async Task SendAsync_SynchronouslyBlockingObserverIsRacedByHardDeadlineAndInvokedOnce()
    {
        var timeProvider = new CallbackTimeProvider();
        var timeoutEvents = new ConcurrentQueue<HttpTimeoutTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            totalTimeout: TimeSpan.FromSeconds(1),
            onTimeout: (value, _) =>
            {
                timeoutEvents.Enqueue(value);
                return ValueTask.CompletedTask;
            });
        using var releaseObserver = new ManualResetEventSlim();
        var observerEntered = NewSignal();
        var requestContent = new TrackingContent();
        var responseContent = new TrackingContent();
        var observerCalls = 0;
        var operation = StartPipelineCall(() => pipeline.SendAsync(
            (_, _) => ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/observer")
            {
                Content = requestContent,
            }),
            (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = responseContent,
            }),
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            (_, _) =>
            {
                Interlocked.Increment(ref observerCalls);
                observerEntered.TrySetResult();
                releaseObserver.Wait(CancellationToken.None);
                return ValueTask.CompletedTask;
            },
            TestContext.Current.CancellationToken));

        await observerEntered.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        var promptFailure = await CaptureBeforeReleaseAsync(
            operation,
            releaseObserver,
            () =>
            {
                requestContent.Disposed.ShouldBeFalse();
                responseContent.Disposed.ShouldBeFalse();
            });
        await WaitForConditionAsync(() => requestContent.Disposed && responseContent.Disposed);
        await ObserveAndDisposeResponseAsync(operation);

        promptFailure.ShouldBeOfType<HttpRequestTimeoutException>();
        timeoutEvents.Count.ShouldBe(1);
        observerCalls.ShouldBe(1);
    }

    [Theory]
    [InlineData(SynchronousBoundary.Factory)]
    [InlineData(SynchronousBoundary.Sender)]
    [InlineData(SynchronousBoundary.Observer)]
    public async Task SendAsync_SynchronouslyBlockingDelegatePreservesExactCallerCancellation(
        SynchronousBoundary boundary)
    {
        var timeProvider = new CallbackTimeProvider();
        var retryEvents = new ConcurrentQueue<HttpRetryTelemetry>();
        var circuitEvents = new ConcurrentQueue<HttpCircuitTelemetry>();
        var pipeline = CreatePipeline(
            timeProvider,
            maxAttempts: 2,
            circuitMinimumThroughput: 2,
            onRetry: (value, _) =>
            {
                retryEvents.Enqueue(value);
                return ValueTask.CompletedTask;
            },
            onCircuit: (value, _) =>
            {
                circuitEvents.Enqueue(value);
                return ValueTask.CompletedTask;
            });
        using var cancellation = new CancellationTokenSource();
        using var releaseDelegate = new ManualResetEventSlim();
        var delegateEntered = NewSignal();
        var requestContent = new TrackingContent();
        var responseContent = new TrackingContent();
        var observerCalls = 0;

        ValueTask<HttpRequestMessage> Factory(int _, CancellationToken __)
        {
            if (boundary == SynchronousBoundary.Factory)
            {
                delegateEntered.TrySetResult();
                releaseDelegate.Wait(CancellationToken.None);
            }

            return ValueTask.FromResult(new HttpRequestMessage(HttpMethod.Get, "https://example.test/caller-cancellation")
            {
                Content = requestContent,
            });
        }

        Task<HttpResponseMessage> Sender(
            HttpRequestMessage _,
            HttpCompletionOption __,
            CancellationToken ___)
        {
            if (boundary == SynchronousBoundary.Sender)
            {
                delegateEntered.TrySetResult();
                releaseDelegate.Wait(CancellationToken.None);
            }

            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = responseContent,
            });
        }

        ValueTask Observer(HttpResponseMessage _, CancellationToken __)
        {
            Interlocked.Increment(ref observerCalls);
            if (boundary == SynchronousBoundary.Observer)
            {
                delegateEntered.TrySetResult();
                releaseDelegate.Wait(CancellationToken.None);
            }

            return ValueTask.CompletedTask;
        }

        var operation = StartPipelineCall(() => pipeline.SendAsync(
            Factory,
            Sender,
            HttpCompletionOption.ResponseHeadersRead,
            HttpRequestReplaySafety.Replayable,
            Observer,
            cancellation.Token));

        await delegateEntered.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        cancellation.Cancel();
        var promptFailure = await CaptureBeforeReleaseAsync(operation, releaseDelegate);
        await WaitForConditionAsync(() => requestContent.Disposed);
        if (boundary != SynchronousBoundary.Factory)
        {
            await WaitForConditionAsync(() => responseContent.Disposed && Volatile.Read(ref observerCalls) == 1);
        }
        await ObserveAndDisposeResponseAsync(operation);

        var canceled = promptFailure.ShouldBeOfType<OperationCanceledException>();
        canceled.CancellationToken.ShouldBe(cancellation.Token);
        retryEvents.ShouldBeEmpty();
        circuitEvents.ShouldBeEmpty();
    }

    private static HttpRequestResiliencePipeline CreatePipeline(
        CallbackTimeProvider timeProvider,
        int maxAttempts = 1,
        TimeSpan? totalTimeout = null,
        double circuitFailureRatio = 0.5,
        int circuitMinimumThroughput = 20,
        Func<HttpRetryTelemetry, CancellationToken, ValueTask>? onRetry = null,
        Func<HttpTimeoutTelemetry, CancellationToken, ValueTask>? onTimeout = null,
        Func<HttpCircuitTelemetry, CancellationToken, ValueTask>? onCircuit = null)
        => new("critical-fix-2", new HttpRequestResilienceOptions
        {
            MaxAttempts = maxAttempts,
            TotalRequestTimeout = totalTimeout ?? TimeSpan.MaxValue,
            BackoffBaseDelay = TimeSpan.FromMilliseconds(1),
            MaximumDelay = TimeSpan.FromMilliseconds(1),
            CircuitFailureRatio = circuitFailureRatio,
            CircuitMinimumThroughput = circuitMinimumThroughput,
            CircuitSamplingDuration = TimeSpan.FromSeconds(30),
            CircuitBreakDuration = TimeSpan.FromSeconds(5),
            TimeProvider = timeProvider,
            JitterProvider = () => 0,
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

    private static async Task TripCircuitAsync(HttpRequestResiliencePipeline pipeline)
    {
        for (var i = 0; i < 2; i++)
        {
            using var failure = await SendAsync(
                pipeline,
                (_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        }
    }

    private static Task<HttpResponseMessage> StartPipelineCall(Func<Task<HttpResponseMessage>> start)
        => Task.Run(start);

    private static async Task<Exception?> CaptureBeforeReleaseAsync(
        Task<HttpResponseMessage> operation,
        ManualResetEventSlim release,
        Action? beforeRelease = null)
    {
        try
        {
            var failure = await Record.ExceptionAsync(async () =>
            {
                using var response = await operation.WaitAsync(
                    PromptSafetyTimeout,
                    TestContext.Current.CancellationToken);
            });
            beforeRelease?.Invoke();
            return failure;
        }
        finally
        {
            release.Set();
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
        for (var i = 0; i < 10_000 && !condition(); i++)
        {
            await Task.Yield();
        }

        condition().ShouldBeTrue();
    }

    public enum SynchronousBoundary
    {
        Factory,
        Sender,
        Observer,
    }

    private sealed class TrackingContent : HttpContent
    {
        private int _disposed;

        public bool Disposed => Volatile.Read(ref _disposed) != 0;

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context)
            => Task.CompletedTask;

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

    private sealed class CallbackTimeProvider : TimeProvider
    {
        private readonly object _gate = new();
        private readonly List<ManualTimer> _timers = [];
        private long _timestamp;
        private DateTimeOffset _utcNow = DateTimeOffset.UnixEpoch;
        private Action? _nextTimestampCallback;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp()
        {
            long timestamp;
            lock (_gate)
            {
                timestamp = _timestamp;
            }

            Interlocked.Exchange(ref _nextTimestampCallback, null)?.Invoke();
            return timestamp;
        }

        public override DateTimeOffset GetUtcNow()
        {
            lock (_gate)
            {
                return _utcNow;
            }
        }

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period)
        {
            ArgumentNullException.ThrowIfNull(callback);
            lock (_gate)
            {
                var timer = new ManualTimer(this, callback, state, dueTime, period);
                _timers.Add(timer);
                return timer;
            }
        }

        public void InvokeOnNextTimestamp(Action callback)
        {
            ArgumentNullException.ThrowIfNull(callback);
            Interlocked.Exchange(ref _nextTimestampCallback, callback).ShouldBeNull();
        }

        public void Advance(TimeSpan duration)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(duration, TimeSpan.Zero);
            List<(TimerCallback Callback, object? State)> callbacks = [];
            lock (_gate)
            {
                _timestamp = checked(_timestamp + duration.Ticks);
                _utcNow += duration;
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

        private sealed class ManualTimer : ITimer
        {
            private readonly CallbackTimeProvider _owner;
            private readonly TimerCallback _callback;
            private readonly object? _state;
            private long? _dueTimestamp;
            private TimeSpan _period;

            public ManualTimer(
                CallbackTimeProvider owner,
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

            public bool TryFire(
                long timestamp,
                out (TimerCallback Callback, object? State) callback)
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
