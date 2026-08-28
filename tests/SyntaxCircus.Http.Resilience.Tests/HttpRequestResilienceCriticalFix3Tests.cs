namespace SyntaxCircus.Http.Resilience.Tests;

public class HttpRequestResilienceCriticalFix3Tests
{
    private static readonly TimeSpan PromptSafetyTimeout = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task SendAsync_LateSenderRacingCallerCancellationDoesNotWaitForBlockingObserver()
    {
        var timeProvider = new ManualTimeProvider();
        var timeoutCalls = 0;
        var circuitCalls = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            onTimeout: (_, _) =>
            {
                Interlocked.Increment(ref timeoutCalls);
                return ValueTask.CompletedTask;
            },
            onCircuit: (_, _) =>
            {
                Interlocked.Increment(ref circuitCalls);
                return ValueTask.CompletedTask;
            });
        using var cancellation = new CancellationTokenSource();
        using var continuationGate = new ExecutionContextGate();
        using var releaseObserver = new BlockingGate();
        var senderEntered = NewSignal();
        var senderCompletion = new TaskCompletionSource<HttpResponseMessage>();
        var observerEntered = NewSignal();
        var requestContent = new TrackingContent();
        var responseContent = new TrackingContent();
        using var originalRequest = new HttpRequestMessage(HttpMethod.Get, "https://example.test/cancel-late")
        {
            Content = requestContent,
        };
        using var originalResponse = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = responseContent,
        };
        HttpResponseMessage? observedResponse = null;
        var observerCalls = 0;

        Task<HttpResponseMessage> operation;
        using (continuationGate.Install())
        {
            operation = pipeline.SendAsync(
                (_, _) => ValueTask.FromResult(originalRequest),
                (_, _, _) =>
                {
                    senderEntered.TrySetResult();
                    return senderCompletion.Task;
                },
                HttpCompletionOption.ResponseHeadersRead,
                HttpRequestReplaySafety.Replayable,
                (response, _) =>
                {
                    observedResponse = response;
                    Interlocked.Increment(ref observerCalls);
                    observerEntered.TrySetResult();
                    releaseObserver.Wait();
                    return ValueTask.CompletedTask;
                },
                cancellation.Token);
        }

        await senderEntered.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        continuationGate.Arm(ignoreCurrentThread: true);
        cancellation.Cancel();
        await continuationGate.Entered.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        senderCompletion.SetResult(originalResponse);
        continuationGate.Release();

        await observerEntered.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        requestContent.Disposed.ShouldBeFalse();
        responseContent.Disposed.ShouldBeFalse();

        var failure = await CaptureBeforeReleaseAsync(operation, releaseObserver);
        await Task.WhenAll(requestContent.DisposedTask, responseContent.DisposedTask)
            .WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);

        var canceled = failure.ShouldBeOfType<OperationCanceledException>();
        canceled.CancellationToken.ShouldBe(cancellation.Token);
        observerCalls.ShouldBe(1);
        observedResponse.ShouldBeSameAs(originalResponse);
        timeoutCalls.ShouldBe(0);
        circuitCalls.ShouldBe(0);
    }

    [Fact]
    public async Task SendAsync_LateSenderRacingDeadlineDoesNotWaitForBlockingObserver()
    {
        var timeProvider = new ManualTimeProvider();
        var timeoutCalls = 0;
        var circuitCalls = 0;
        var pipeline = CreatePipeline(
            timeProvider,
            TimeSpan.FromSeconds(1),
            (_, _) =>
            {
                Interlocked.Increment(ref timeoutCalls);
                return ValueTask.CompletedTask;
            },
            (_, _) =>
            {
                Interlocked.Increment(ref circuitCalls);
                return ValueTask.CompletedTask;
            });
        using var continuationGate = new ExecutionContextGate();
        using var releaseObserver = new BlockingGate();
        var senderEntered = NewSignal();
        var senderCompletion = new TaskCompletionSource<HttpResponseMessage>();
        var observerEntered = NewSignal();
        var requestContent = new TrackingContent();
        var responseContent = new TrackingContent();
        using var originalRequest = new HttpRequestMessage(HttpMethod.Get, "https://example.test/deadline-late")
        {
            Content = requestContent,
        };
        using var originalResponse = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = responseContent,
        };
        HttpResponseMessage? observedResponse = null;
        var observerCalls = 0;

        Task<HttpResponseMessage> operation;
        using (continuationGate.Install())
        {
            operation = pipeline.SendAsync(
                (_, _) => ValueTask.FromResult(originalRequest),
                (_, _, _) =>
                {
                    senderEntered.TrySetResult();
                    return senderCompletion.Task;
                },
                HttpCompletionOption.ResponseHeadersRead,
                HttpRequestReplaySafety.Replayable,
                (response, _) =>
                {
                    observedResponse = response;
                    Interlocked.Increment(ref observerCalls);
                    observerEntered.TrySetResult();
                    releaseObserver.Wait();
                    return ValueTask.CompletedTask;
                },
                TestContext.Current.CancellationToken);
        }

        await senderEntered.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        continuationGate.Arm(ignoreCurrentThread: true);
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await continuationGate.Entered.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        senderCompletion.SetResult(originalResponse);
        continuationGate.Release();

        await observerEntered.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        requestContent.Disposed.ShouldBeFalse();
        responseContent.Disposed.ShouldBeFalse();

        var failure = await CaptureBeforeReleaseAsync(operation, releaseObserver);
        await Task.WhenAll(requestContent.DisposedTask, responseContent.DisposedTask)
            .WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);

        failure.ShouldBeOfType<HttpRequestTimeoutException>();
        observerCalls.ShouldBe(1);
        observedResponse.ShouldBeSameAs(originalResponse);
        timeoutCalls.ShouldBe(1);
        circuitCalls.ShouldBe(0);
    }

    [Fact]
    public async Task SendAsync_QueuedSenderReceivesStableRequestAfterCancellationTransfersOwnership()
    {
        var timeProvider = new ManualTimeProvider();
        var pipeline = CreatePipeline(timeProvider);
        using var cancellation = new CancellationTokenSource();
        using var senderGate = new ExecutionContextGate();
        var requestContent = new TrackingContent();
        var responseContent = new TrackingContent();
        using var originalRequest = new HttpRequestMessage(HttpMethod.Get, "https://example.test/queued-sender")
        {
            Content = requestContent,
        };
        using var originalResponse = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = responseContent,
        };
        HttpRequestMessage? receivedRequest = null;
        var senderFinished = NewSignal();

        Task<HttpResponseMessage> operation;
        using (senderGate.Install())
        {
            operation = pipeline.SendAsync(
                (_, _) =>
                {
                    senderGate.Arm(entriesToSkip: 1);
                    return ValueTask.FromResult(originalRequest);
                },
                (request, _, _) =>
                {
                    receivedRequest = request;
                    senderFinished.TrySetResult();
                    return Task.FromResult(originalResponse);
                },
                HttpCompletionOption.ResponseHeadersRead,
                HttpRequestReplaySafety.Replayable,
                cancellationToken: cancellation.Token);
        }

        await senderGate.Entered.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        receivedRequest.ShouldBeNull();
        cancellation.Cancel();

        Exception? failure;
        try
        {
            failure = await CaptureAsync(operation);
            requestContent.Disposed.ShouldBeFalse();
            responseContent.Disposed.ShouldBeFalse();
        }
        finally
        {
            senderGate.Release();
        }

        await senderFinished.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        await Task.WhenAll(requestContent.DisposedTask, responseContent.DisposedTask)
            .WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);

        var canceled = failure.ShouldBeOfType<OperationCanceledException>();
        canceled.CancellationToken.ShouldBe(cancellation.Token);
        receivedRequest.ShouldBeSameAs(originalRequest);
    }

    [Fact]
    public async Task SendAsync_QueuedObserverReceivesStableResponseAfterDeadlineTransfersOwnership()
    {
        var timeProvider = new ManualTimeProvider();
        var pipeline = CreatePipeline(timeProvider, TimeSpan.FromSeconds(1));
        using var observerGate = new ExecutionContextGate();
        var requestContent = new TrackingContent();
        var responseContent = new TrackingContent();
        using var originalRequest = new HttpRequestMessage(HttpMethod.Get, "https://example.test/queued-observer")
        {
            Content = requestContent,
        };
        using var originalResponse = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = responseContent,
        };
        HttpResponseMessage? receivedResponse = null;
        var observerCalls = 0;
        var observerFinished = NewSignal();

        Task<HttpResponseMessage> operation;
        using (observerGate.Install())
        {
            operation = pipeline.SendAsync(
                (_, _) => ValueTask.FromResult(originalRequest),
                (_, _, _) =>
                {
                    observerGate.Arm(entriesToSkip: 1);
                    return Task.FromResult(originalResponse);
                },
                HttpCompletionOption.ResponseHeadersRead,
                HttpRequestReplaySafety.Replayable,
                (response, _) =>
                {
                    receivedResponse = response;
                    Interlocked.Increment(ref observerCalls);
                    observerFinished.TrySetResult();
                    return ValueTask.CompletedTask;
                },
                TestContext.Current.CancellationToken);
        }

        await observerGate.Entered.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        observerCalls.ShouldBe(0);
        timeProvider.Advance(TimeSpan.FromSeconds(1));

        Exception? failure;
        try
        {
            failure = await CaptureAsync(operation);
            requestContent.Disposed.ShouldBeFalse();
            responseContent.Disposed.ShouldBeFalse();
        }
        finally
        {
            observerGate.Release();
        }

        await observerFinished.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        await Task.WhenAll(requestContent.DisposedTask, responseContent.DisposedTask)
            .WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);

        failure.ShouldBeOfType<HttpRequestTimeoutException>();
        observerCalls.ShouldBe(1);
        receivedResponse.ShouldBeSameAs(originalResponse);
    }

    private static HttpRequestResiliencePipeline CreatePipeline(
        ManualTimeProvider timeProvider,
        TimeSpan? totalTimeout = null,
        Func<HttpTimeoutTelemetry, CancellationToken, ValueTask>? onTimeout = null,
        Func<HttpCircuitTelemetry, CancellationToken, ValueTask>? onCircuit = null)
        => new("critical-fix-3", new HttpRequestResilienceOptions
        {
            MaxAttempts = 1,
            TotalRequestTimeout = totalTimeout ?? TimeSpan.MaxValue,
            BackoffBaseDelay = TimeSpan.FromMilliseconds(1),
            MaximumDelay = TimeSpan.FromMilliseconds(1),
            CircuitMinimumThroughput = 20,
            TimeProvider = timeProvider,
            JitterProvider = () => 0,
            OnTimeout = onTimeout,
            OnCircuitStateChanged = onCircuit,
        });

    private static async Task<Exception?> CaptureBeforeReleaseAsync(
        Task<HttpResponseMessage> operation,
        BlockingGate release)
    {
        try
        {
            return await CaptureAsync(operation);
        }
        finally
        {
            release.Release();
        }
    }

    private static async Task<Exception?> CaptureAsync(Task<HttpResponseMessage> operation)
        => await Record.ExceptionAsync(async () =>
        {
            using var response = await operation.WaitAsync(
                PromptSafetyTimeout,
                TestContext.Current.CancellationToken);
        });

    private static TaskCompletionSource NewSignal()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class BlockingGate : IDisposable
    {
        private readonly ManualResetEventSlim _release = new();

        public void Wait() => _release.Wait(CancellationToken.None);

        public void Release() => _release.Set();

        public void Dispose()
        {
            _release.Set();
            _release.Dispose();
        }
    }

    private sealed class TrackingContent : HttpContent
    {
        private readonly TaskCompletionSource _disposed = NewSignal();
        private int _isDisposed;

        public bool Disposed => Volatile.Read(ref _isDisposed) != 0;

        public Task DisposedTask => _disposed.Task;

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context)
            => Task.CompletedTask;

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return true;
        }

        protected override void Dispose(bool disposing)
        {
            Interlocked.Exchange(ref _isDisposed, 1);
            _disposed.TrySetResult();
            base.Dispose(disposing);
        }
    }

    private sealed class ExecutionContextGate : IDisposable
    {
        private static readonly AsyncLocal<ExecutionContextGate?> Current = new(OnContextChanged);

        private readonly ManualResetEventSlim _release = new();
        private readonly TaskCompletionSource _entered = NewSignal();
        private int _armed;
        private int _armingThreadId;
        private int _entriesToSkip;
        private int _used;

        public Task Entered => _entered.Task;

        public IDisposable Install()
        {
            var previous = Current.Value;
            Current.Value = this;
            return new Scope(previous);
        }

        public void Arm(bool ignoreCurrentThread = false, int entriesToSkip = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(entriesToSkip);
            Volatile.Write(
                ref _armingThreadId,
                ignoreCurrentThread ? Environment.CurrentManagedThreadId : -1);
            Volatile.Write(ref _entriesToSkip, entriesToSkip);
            Volatile.Write(ref _armed, 1);
        }

        public void Release() => _release.Set();

        public void Dispose()
        {
            _release.Set();
            _release.Dispose();
        }

        private static void OnContextChanged(AsyncLocalValueChangedArgs<ExecutionContextGate?> args)
        {
            var gate = args.CurrentValue;
            if (!args.ThreadContextChanged
                || gate is null
                || Volatile.Read(ref gate._armed) == 0
                || Environment.CurrentManagedThreadId == Volatile.Read(ref gate._armingThreadId))
            {
                return;
            }

            while (true)
            {
                var entriesToSkip = Volatile.Read(ref gate._entriesToSkip);
                if (entriesToSkip == 0)
                {
                    break;
                }

                if (Interlocked.CompareExchange(
                    ref gate._entriesToSkip,
                    entriesToSkip - 1,
                    entriesToSkip) == entriesToSkip)
                {
                    return;
                }
            }

            if (Interlocked.CompareExchange(ref gate._used, 1, 0) != 0)
            {
                return;
            }

            gate._entered.TrySetResult();
            gate._release.Wait(CancellationToken.None);
        }

        private sealed class Scope(ExecutionContextGate? previous) : IDisposable
        {
            private int _disposed;

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 0)
                {
                    Current.Value = previous;
                }
            }
        }
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        private readonly object _gate = new();
        private readonly List<ManualTimer> _timers = [];
        private long _timestamp;
        private DateTimeOffset _utcNow = DateTimeOffset.UnixEpoch;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

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
