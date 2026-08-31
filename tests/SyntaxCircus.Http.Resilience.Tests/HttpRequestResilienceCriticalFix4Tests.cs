namespace SyntaxCircus.Http.Resilience.Tests;

public class HttpRequestResilienceCriticalFix4Tests
{
    private static readonly TimeSpan PromptSafetyTimeout = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task SendAsync_TimeoutCompletionThreadDoesNotRunLateObserverAfterOwnerAwaitsSender()
    {
        var timeProvider = new ManualTimeProvider();
        using var suspensionProbe = new ExecutionContextSuspensionProbe();
        using var releaseObserver = new BlockingGate();
        var senderEntered = NewSignal();
        var senderSuspended = NewSignal();
        var lateOwnerSuspended = NewSignal();
        var senderCompletion = new TaskCompletionSource<HttpResponseMessage>();
        var completionReturned = NewSignal();
        var observerEntered = NewSignal();
        var requestContent = new TrackingContent();
        var responseContent = new TrackingContent();
        using var originalRequest = new HttpRequestMessage(HttpMethod.Get, "https://example.test/reentry-timeout")
        {
            Content = requestContent,
        };
        using var originalResponse = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = responseContent,
        };
        var retryCalls = 0;
        var timeoutCalls = 0;
        var circuitCalls = 0;
        var observerCalls = 0;
        HttpResponseMessage? observedResponse = null;
        var pipeline = CreatePipeline(
            timeProvider,
            TimeSpan.FromSeconds(1),
            onRetry: (_, _) =>
            {
                Interlocked.Increment(ref retryCalls);
                return ValueTask.CompletedTask;
            },
            onTimeout: (_, _) =>
            {
                Interlocked.Increment(ref timeoutCalls);
                lateOwnerSuspended.Task.Wait(PromptSafetyTimeout, TestContext.Current.CancellationToken)
                    .ShouldBeTrue();
                senderCompletion.SetResult(originalResponse);
                completionReturned.TrySetResult();
                return ValueTask.CompletedTask;
            },
            onCircuit: (_, _) =>
            {
                Interlocked.Increment(ref circuitCalls);
                return ValueTask.CompletedTask;
            });

        Task<HttpResponseMessage> operation;
        using (suspensionProbe.Install())
        {
            operation = pipeline.SendAsync(
                (_, _) => ValueTask.FromResult(originalRequest),
                (_, _, _) =>
                {
                    suspensionProbe.WatchCurrentTaskSuspension(senderSuspended);
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
        await senderSuspended.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        suspensionProbe.WatchNextTaskSuspension(lateOwnerSuspended);
        timeProvider.Advance(TimeSpan.FromSeconds(1));

        await observerEntered.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        requestContent.Disposed.ShouldBeFalse();
        responseContent.Disposed.ShouldBeFalse();

        var terminal = await CaptureTerminalBeforeReleaseAsync(operation, completionReturned.Task, releaseObserver);
        await Task.WhenAll(requestContent.DisposedTask, responseContent.DisposedTask)
            .WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);

        terminal.ReturnedBeforeRelease.ShouldBeTrue();
        var timeout = terminal.Failure.ShouldBeOfType<HttpRequestTimeoutException>();
        timeout.PipelineName.ShouldBe("critical-fix-4");
        timeout.Timeout.ShouldBe(TimeSpan.FromSeconds(1));
        observerCalls.ShouldBe(1);
        observedResponse.ShouldBeSameAs(originalResponse);
        retryCalls.ShouldBe(0);
        timeoutCalls.ShouldBe(1);
        circuitCalls.ShouldBe(0);
    }

    [Fact]
    public async Task SendAsync_CancellationControl_PreservesExactTokenAndDeferredLateOwnership()
    {
        var timeProvider = new ManualTimeProvider();
        using var suspensionProbe = new ExecutionContextSuspensionProbe();
        using var releaseObserver = new BlockingGate();
        using var cancellation = new CancellationTokenSource();
        var senderEntered = NewSignal();
        var senderSuspended = NewSignal();
        var senderCompletion = new TaskCompletionSource<HttpResponseMessage>();
        var completionReturned = NewSignal();
        var observerEntered = NewSignal();
        var requestContent = new TrackingContent();
        var responseContent = new TrackingContent();
        using var originalRequest = new HttpRequestMessage(HttpMethod.Get, "https://example.test/reentry-cancellation")
        {
            Content = requestContent,
        };
        using var originalResponse = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = responseContent,
        };
        var retryCalls = 0;
        var timeoutCalls = 0;
        var circuitCalls = 0;
        var observerCalls = 0;
        HttpResponseMessage? observedResponse = null;
        var pipeline = CreatePipeline(
            timeProvider,
            onRetry: (_, _) =>
            {
                Interlocked.Increment(ref retryCalls);
                return ValueTask.CompletedTask;
            },
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

        Task<HttpResponseMessage> operation;
        using (suspensionProbe.Install())
        {
            operation = pipeline.SendAsync(
                (_, _) => ValueTask.FromResult(originalRequest),
                (_, _, _) =>
                {
                    suspensionProbe.WatchCurrentTaskSuspension(senderSuspended);
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
        await senderSuspended.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        // This is a cancellation/ownership control. Unlike the timeout case above, it does not
        // infer or assert which detached owner is suspended after cancellation registration.
        var completionThread = Task.Run(
            () =>
            {
                cancellation.Cancel();
                senderCompletion.SetResult(originalResponse);
                completionReturned.TrySetResult();
            },
            TestContext.Current.CancellationToken);

        await observerEntered.Task.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        requestContent.Disposed.ShouldBeFalse();
        responseContent.Disposed.ShouldBeFalse();

        var terminal = await CaptureTerminalBeforeReleaseAsync(operation, completionReturned.Task, releaseObserver);
        await completionThread.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        await Task.WhenAll(requestContent.DisposedTask, responseContent.DisposedTask)
            .WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);

        terminal.ReturnedBeforeRelease.ShouldBeTrue();
        var canceled = terminal.Failure.ShouldBeOfType<OperationCanceledException>();
        canceled.CancellationToken.ShouldBe(cancellation.Token);
        observerCalls.ShouldBe(1);
        observedResponse.ShouldBeSameAs(originalResponse);
        retryCalls.ShouldBe(0);
        timeoutCalls.ShouldBe(0);
        circuitCalls.ShouldBe(0);
    }

    private static HttpRequestResiliencePipeline CreatePipeline(
        ManualTimeProvider timeProvider,
        TimeSpan? totalTimeout = null,
        Func<HttpRetryTelemetry, CancellationToken, ValueTask>? onRetry = null,
        Func<HttpTimeoutTelemetry, CancellationToken, ValueTask>? onTimeout = null,
        Func<HttpCircuitTelemetry, CancellationToken, ValueTask>? onCircuit = null)
        => new("critical-fix-4", new HttpRequestResilienceOptions
        {
            MaxAttempts = 1,
            TotalRequestTimeout = totalTimeout ?? TimeSpan.MaxValue,
            BackoffBaseDelay = TimeSpan.FromMilliseconds(1),
            MaximumDelay = TimeSpan.FromMilliseconds(1),
            CircuitMinimumThroughput = 20,
            TimeProvider = timeProvider,
            JitterProvider = () => 0,
            OnRetry = onRetry,
            OnTimeout = onTimeout,
            OnCircuitStateChanged = onCircuit,
        });

    private static async Task<TerminalCapture> CaptureTerminalBeforeReleaseAsync(
        Task<HttpResponseMessage> operation,
        Task completionReturned,
        BlockingGate releaseObserver)
    {
        var failureTask = CaptureAsync(operation);
        var bothCompleted = Task.WhenAll(failureTask, completionReturned);
        bool returnedBeforeRelease;
        try
        {
            var safetyFailure = await Record.ExceptionAsync(
                () => bothCompleted.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken));
            returnedBeforeRelease = safetyFailure is null;
        }
        finally
        {
            releaseObserver.Release();
        }

        var failure = await failureTask.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        await completionReturned.WaitAsync(PromptSafetyTimeout, TestContext.Current.CancellationToken);
        return new TerminalCapture(failure, returnedBeforeRelease);
    }

    private static async Task<Exception?> CaptureAsync(Task<HttpResponseMessage> operation)
        => await Record.ExceptionAsync(async () =>
        {
            using var response = await operation;
        });

    private static TaskCompletionSource NewSignal()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly record struct TerminalCapture(Exception? Failure, bool ReturnedBeforeRelease);

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

    private sealed class ExecutionContextSuspensionProbe : IDisposable
    {
        private static readonly AsyncLocal<ExecutionContextSuspensionProbe?> Current = new(OnContextChanged);

        private TaskCompletionSource? _nextTaskSuspension;
        private int _watchedTaskId = -1;

        public IDisposable Install()
        {
            var previous = Current.Value;
            Current.Value = this;
            return new Scope(previous);
        }

        public void WatchCurrentTaskSuspension(TaskCompletionSource signal)
        {
            var taskId = Task.CurrentId;
            taskId.ShouldNotBeNull();
            InstallWatch(signal, taskId);
        }

        public void WatchNextTaskSuspension(TaskCompletionSource signal)
            => InstallWatch(signal, watchedTaskId: null);

        public void Dispose()
        {
            Interlocked.Exchange(ref _nextTaskSuspension, null)?.TrySetCanceled();
        }

        private void InstallWatch(TaskCompletionSource signal, int? watchedTaskId)
        {
            ArgumentNullException.ThrowIfNull(signal);
            Interlocked.CompareExchange(ref _nextTaskSuspension, signal, null).ShouldBeNull();
            Volatile.Write(ref _watchedTaskId, watchedTaskId ?? -1);
        }

        private static void OnContextChanged(
            AsyncLocalValueChangedArgs<ExecutionContextSuspensionProbe?> args)
        {
            var probe = args.PreviousValue;
            if (!args.ThreadContextChanged
                || probe is null
                || ReferenceEquals(args.CurrentValue, probe))
            {
                return;
            }

            var currentTaskId = Task.CurrentId;
            var watchedTaskId = Volatile.Read(ref probe._watchedTaskId);
            if (currentTaskId is null
                || watchedTaskId >= 0 && currentTaskId != watchedTaskId)
            {
                return;
            }

            Interlocked.Exchange(ref probe._nextTaskSuspension, null)?.TrySetResult();
        }

        private sealed class Scope(ExecutionContextSuspensionProbe? previous) : IDisposable
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
