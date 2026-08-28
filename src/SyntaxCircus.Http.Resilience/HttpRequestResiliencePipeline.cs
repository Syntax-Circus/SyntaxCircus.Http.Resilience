using System.Collections.Frozen;
using System.Net.Http;
using System.Numerics;
using System.Runtime.ExceptionServices;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;

namespace SyntaxCircus.Http.Resilience;

public sealed class HttpRequestResiliencePipeline
{
    private static readonly TimeSpan PollyMinimumCircuitDuration = TimeSpan.FromMilliseconds(500) + TimeSpan.FromTicks(1);
    private static readonly TimeSpan PollyMaximumCircuitDuration = TimeSpan.FromDays(1);
    private const long MaximumRelevantCircuitElapsedTicks = TimeSpan.TicksPerDay + 1;
    private static readonly DateTimeOffset CircuitUtcEpoch = DateTimeOffset.UnixEpoch;
    private static readonly ResiliencePropertyKey<ExecutionState> ExecutionStateKey = new("HttpRequestExecutionState");

    private readonly string _name;
    private readonly HttpRequestResilienceOptions _options;
    private readonly ResiliencePipeline<AttemptResult> _pipeline;
    private long _circuitOpenedTimestamp;

    public HttpRequestResiliencePipeline(string name, HttpRequestResilienceOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(options);

        Validate(options);

        _name = name;
        _options = new HttpRequestResilienceOptions
        {
            MaxAttempts = options.MaxAttempts,
            TotalRequestTimeout = options.TotalRequestTimeout,
            BackoffBaseDelay = options.BackoffBaseDelay,
            MaximumDelay = options.MaximumDelay,
            CircuitFailureRatio = options.CircuitFailureRatio,
            CircuitMinimumThroughput = options.CircuitMinimumThroughput,
            CircuitSamplingDuration = options.CircuitSamplingDuration,
            CircuitBreakDuration = options.CircuitBreakDuration,
            TimeProvider = options.TimeProvider,
            JitterProvider = options.JitterProvider,
            RetryableStatusCodes = options.RetryableStatusCodes.ToFrozenSet(),
            RetryableExceptionCategories = options.RetryableExceptionCategories.ToFrozenSet(),
            OnRetry = options.OnRetry,
            OnTimeout = options.OnTimeout,
            OnCircuitStateChanged = options.OnCircuitStateChanged,
        };

        _pipeline = BuildPipeline();
    }

    public Task<HttpResponseMessage> SendAsync(
        Func<int, CancellationToken, ValueTask<HttpRequestMessage>> requestFactory,
        Func<HttpRequestMessage, HttpCompletionOption, CancellationToken, Task<HttpResponseMessage>> sender,
        HttpCompletionOption completionOption,
        HttpRequestReplaySafety replaySafety,
        Func<HttpResponseMessage, CancellationToken, ValueTask>? responseObserver = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(requestFactory);
        ArgumentNullException.ThrowIfNull(sender);

        return SendCoreAsync(
            requestFactory,
            sender,
            completionOption,
            replaySafety,
            responseObserver,
            cancellationToken);
    }

    private ResiliencePipeline<AttemptResult> BuildPipeline()
    {
        var retryPipeline = BuildRetryPipeline();
        var circuitTimeProvider = CircuitTimeProvider.Create(
            _options.TimeProvider,
            _options.CircuitSamplingDuration,
            _options.CircuitBreakDuration);
        var builder = new ResiliencePipelineBuilder<AttemptResult>
        {
            TimeProvider = circuitTimeProvider,
        };

        builder.AddCircuitBreaker(new CircuitBreakerStrategyOptions<AttemptResult>
        {
            FailureRatio = _options.CircuitFailureRatio,
            MinimumThroughput = _options.CircuitMinimumThroughput,
            SamplingDuration = circuitTimeProvider.PollySamplingDuration,
            BreakDuration = circuitTimeProvider.PollyBreakDuration,
            ShouldHandle = args => ValueTask.FromResult(TryClassify(args.Outcome, args.Context.CancellationToken, out _)),
            OnOpened = async args =>
            {
                circuitTimeProvider.EnterOpenState();
                Volatile.Write(ref _circuitOpenedTimestamp, _options.TimeProvider.GetTimestamp());
                var category = TryClassify(args.Outcome, args.Context.CancellationToken, out var classified)
                    ? classified
                    : HttpResilienceFailureCategory.CircuitOpen;
                await InvokeCircuitCallbackSafelyAsync(
                    new HttpCircuitTelemetry(
                        _name,
                        HttpResilienceCircuitState.Open,
                        args.Outcome.Result?.Response.StatusCode,
                        category),
                    args.Context.CancellationToken).ConfigureAwait(false);
            },
            OnHalfOpened = async args =>
            {
                circuitTimeProvider.EnterSamplingState();
                await InvokeCircuitCallbackSafelyAsync(
                    new HttpCircuitTelemetry(
                        _name,
                        HttpResilienceCircuitState.HalfOpen,
                        null,
                        HttpResilienceFailureCategory.CircuitOpen),
                    args.Context.CancellationToken).ConfigureAwait(false);
            },
            OnClosed = async args =>
            {
                circuitTimeProvider.EnterSamplingState();
                await InvokeCircuitCallbackSafelyAsync(
                    new HttpCircuitTelemetry(
                        _name,
                        HttpResilienceCircuitState.Closed,
                        args.Outcome.Result?.Response.StatusCode,
                        ClassifyForTelemetry(args.Outcome, args.Context.CancellationToken)),
                    args.Context.CancellationToken).ConfigureAwait(false);
            },
        });

        builder.AddPipeline(retryPipeline);
        return builder.Build();
    }

    private ResiliencePipeline<AttemptResult> BuildRetryPipeline()
    {
        var builder = new ResiliencePipelineBuilder<AttemptResult>
        {
            TimeProvider = _options.TimeProvider,
        };

        if (_options.MaxAttempts > 1)
        {
            builder.AddRetry(new RetryStrategyOptions<AttemptResult>
            {
                MaxRetryAttempts = _options.MaxAttempts - 1,
                ShouldHandle = args => ValueTask.FromResult(
                    GetExecutionState(args.Context).ReplaySafety == HttpRequestReplaySafety.Replayable
                    && TryClassify(args.Outcome, args.Context.CancellationToken, out _)),
                DelayGenerator = args => ValueTask.FromResult<TimeSpan?>(GetRetryDelay(args)),
                OnRetry = async args =>
                {
                    args.Outcome.Result?.DisposeForRetry();
                    await InvokeRetryCallbackSafelyAsync(
                        new HttpRetryTelemetry(
                            _name,
                            args.AttemptNumber + 1,
                            args.Outcome.Result?.Response.StatusCode,
                            ClassifyForTelemetry(args.Outcome, args.Context.CancellationToken),
                            args.RetryDelay),
                        args.Context.CancellationToken).ConfigureAwait(false);
                },
            });
        }

        return builder.Build();
    }

    private async Task<HttpResponseMessage> SendCoreAsync(
        Func<int, CancellationToken, ValueTask<HttpRequestMessage>> requestFactory,
        Func<HttpRequestMessage, HttpCompletionOption, CancellationToken, Task<HttpResponseMessage>> sender,
        HttpCompletionOption completionOption,
        HttpRequestReplaySafety replaySafety,
        Func<HttpResponseMessage, CancellationToken, ValueTask>? responseObserver,
        CancellationToken cancellationToken)
    {
        var state = new ExecutionState(
            requestFactory,
            sender,
            completionOption,
            replaySafety,
            responseObserver,
            _options.TimeProvider.GetTimestamp(),
            cancellationToken);
        using var timeoutSource = _options.TotalRequestTimeout == TimeSpan.MaxValue
            ? null
            : new CancellationTokenSource(_options.TotalRequestTimeout, _options.TimeProvider);
        using var budgetSource = timeoutSource is null
            ? null
            : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutSource.Token);
        var context = ResilienceContextPool.Shared.Get(budgetSource?.Token ?? cancellationToken);
        context.Properties.Set(ExecutionStateKey, state);

        try
        {
            var result = await _pipeline.ExecuteAsync(
                static (executionContext, executionState) => executionState.SendAttemptAsync(executionContext.CancellationToken),
                context,
                state).ConfigureAwait(false);
            state.ReleaseResponse(result.Response);
            return result.Response;
        }
        catch (ResponseObserverException exception)
        {
            state.DisposePendingResponseBestEffort();
            ExceptionDispatchInfo.Capture(exception.InnerException!).Throw();
            throw;
        }
        catch (OperationCanceledException exception) when (cancellationToken.IsCancellationRequested)
        {
            state.DisposePendingResponseBestEffort();
            throw new OperationCanceledException(exception.Message, exception, cancellationToken);
        }
        catch (OperationCanceledException exception) when (timeoutSource?.IsCancellationRequested == true)
        {
            state.DisposePendingResponseBestEffort();
            await InvokeTimeoutCallbackSafelyAsync(
                new HttpTimeoutTelemetry(
                    _name,
                    HttpResilienceFailureCategory.Timeout,
                    _options.TotalRequestTimeout),
                cancellationToken).ConfigureAwait(false);
            throw new HttpRequestTimeoutException(_name, _options.TotalRequestTimeout, exception);
        }
        catch (BrokenCircuitException exception)
        {
            state.DisposePendingResponseBestEffort();
            throw new HttpCircuitOpenException(_name, GetCircuitRetryAfter(), exception);
        }
        catch
        {
            state.DisposePendingResponseBestEffort();
            throw;
        }
        finally
        {
            ResilienceContextPool.Shared.Return(context);
        }
    }

    private TimeSpan GetRetryDelay(RetryDelayGeneratorArguments<AttemptResult> args)
    {
        var state = GetExecutionState(args.Context);
        var delay = TryGetRetryAfter(args.Outcome.Result?.Response, out var retryAfter)
            ? retryAfter
            : GetBackoffDelay(args.AttemptNumber);
        var remaining = _options.TotalRequestTimeout - _options.TimeProvider.GetElapsedTime(state.StartTimestamp);

        if (remaining <= TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        return Min(delay, _options.MaximumDelay, remaining);
    }

    private TimeSpan GetBackoffDelay(int retryNumber)
    {
        var jitter = _options.JitterProvider();
        if (!double.IsFinite(jitter) || jitter is < 0 or >= 1)
        {
            throw new InvalidOperationException("JitterProvider must return a finite value greater than or equal to 0 and less than 1.");
        }

        var exponentialMultiplier = Math.Pow(2, retryNumber);
        var ticks = _options.BackoffBaseDelay.Ticks * exponentialMultiplier * (1 + jitter);
        if (ticks >= TimeSpan.MaxValue.Ticks)
        {
            return _options.MaximumDelay;
        }

        return TimeSpan.FromTicks((long)ticks);
    }

    private bool TryGetRetryAfter(HttpResponseMessage? response, out TimeSpan delay)
    {
        var retryAfter = response?.Headers.RetryAfter;
        if (retryAfter?.Delta is { } delta && delta > TimeSpan.Zero)
        {
            delay = delta;
            return true;
        }

        if (retryAfter?.Date is { } date)
        {
            var dateDelay = date - _options.TimeProvider.GetUtcNow();
            if (dateDelay > TimeSpan.Zero)
            {
                delay = dateDelay;
                return true;
            }
        }

        delay = default;
        return false;
    }

    private static TimeSpan Min(TimeSpan first, TimeSpan second, TimeSpan third)
        => first <= second
            ? first <= third ? first : third
            : second <= third ? second : third;

    private TimeSpan? GetCircuitRetryAfter()
    {
        var openedTimestamp = Volatile.Read(ref _circuitOpenedTimestamp);
        if (openedTimestamp == 0)
        {
            return null;
        }

        var remaining = _options.CircuitBreakDuration - _options.TimeProvider.GetElapsedTime(openedTimestamp);
        return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
    }

    private static ExecutionState GetExecutionState(ResilienceContext context)
        => context.Properties.TryGetValue(ExecutionStateKey, out var state)
            ? state
            : throw new InvalidOperationException("The request execution state is unavailable.");

    private bool TryClassify(
        Outcome<AttemptResult> outcome,
        CancellationToken cancellationToken,
        out HttpResilienceFailureCategory category)
        => HttpResilienceOutcomeClassifier.TryClassify(
            outcome.Result?.Response,
            outcome.Exception,
            _options.RetryableStatusCodes,
            _options.RetryableExceptionCategories,
            cancellationToken,
            out category);

    private HttpResilienceFailureCategory ClassifyForTelemetry(
        Outcome<AttemptResult> outcome,
        CancellationToken cancellationToken)
        => TryClassify(outcome, cancellationToken, out var category)
            ? category
            : outcome.Exception is not null
                ? HttpResilienceFailureCategory.Transport
                : HttpResilienceFailureCategory.HttpStatus;

    private async ValueTask InvokeRetryCallbackSafelyAsync(
        HttpRetryTelemetry telemetry,
        CancellationToken cancellationToken)
    {
        if (_options.OnRetry is null)
        {
            return;
        }

        try
        {
            await _options.OnRetry(telemetry, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
        }
    }

    private async ValueTask InvokeCircuitCallbackSafelyAsync(
        HttpCircuitTelemetry telemetry,
        CancellationToken cancellationToken)
    {
        if (_options.OnCircuitStateChanged is null)
        {
            return;
        }

        try
        {
            await _options.OnCircuitStateChanged(telemetry, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
        }
    }

    private async ValueTask InvokeTimeoutCallbackSafelyAsync(
        HttpTimeoutTelemetry telemetry,
        CancellationToken cancellationToken)
    {
        if (_options.OnTimeout is null)
        {
            return;
        }

        try
        {
            await _options.OnTimeout(telemetry, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
        }
    }

    private sealed class AttemptResult(HttpResponseMessage response, ExecutionState state)
    {
        public HttpResponseMessage Response { get; } = response;

        public void DisposeForRetry()
        {
            try
            {
                DisposeBestEffort(Response);
            }
            finally
            {
                state.ReleaseResponse(Response);
            }
        }

        private static void DisposeBestEffort(HttpResponseMessage disposable)
        {
            try
            {
                disposable.Dispose();
            }
            catch
            {
            }
        }
    }

    private sealed class ExecutionState(
        Func<int, CancellationToken, ValueTask<HttpRequestMessage>> requestFactory,
        Func<HttpRequestMessage, HttpCompletionOption, CancellationToken, Task<HttpResponseMessage>> sender,
        HttpCompletionOption completionOption,
        HttpRequestReplaySafety replaySafety,
        Func<HttpResponseMessage, CancellationToken, ValueTask>? responseObserver,
        long startTimestamp,
        CancellationToken callerCancellationToken)
    {
        private HttpResponseMessage? _pendingResponse;
        private int _attemptNumber;

        public HttpRequestReplaySafety ReplaySafety { get; } = replaySafety;

        public long StartTimestamp { get; } = startTimestamp;

        public async ValueTask<AttemptResult> SendAttemptAsync(CancellationToken cancellationToken)
        {
            var attemptNumber = Interlocked.Increment(ref _attemptNumber);
            HttpRequestMessage? request = null;
            try
            {
                request = await requestFactory(attemptNumber, cancellationToken).ConfigureAwait(false)
                    ?? throw new InvalidOperationException("The request factory returned null.");
                var response = await sender(request, completionOption, cancellationToken).ConfigureAwait(false)
                    ?? throw new InvalidOperationException("The sender returned null.");
                Volatile.Write(ref _pendingResponse, response);

                if (responseObserver is not null)
                {
                    try
                    {
                        await responseObserver(response, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        DisposePendingResponseBestEffort();
                        DisposeBestEffort(request);
                        request = null;
                        if (callerCancellationToken.IsCancellationRequested)
                        {
                            throw new OperationCanceledException("The operation was canceled.", exception, callerCancellationToken);
                        }

                        throw new ResponseObserverException(exception);
                    }
                }

                if (callerCancellationToken.IsCancellationRequested)
                {
                    DisposePendingResponseBestEffort();
                    throw new OperationCanceledException(callerCancellationToken);
                }

                return new AttemptResult(response, this);
            }
            catch (Exception exception) when (callerCancellationToken.IsCancellationRequested)
            {
                DisposePendingResponseBestEffort();
                throw new OperationCanceledException("The operation was canceled.", exception, callerCancellationToken);
            }
            finally
            {
                DisposeBestEffort(request);
            }
        }

        public void ReleaseResponse(HttpResponseMessage response)
        {
            Interlocked.CompareExchange(ref _pendingResponse, null, response);
        }

        public void DisposePendingResponseBestEffort()
        {
            DisposeBestEffort(Interlocked.Exchange(ref _pendingResponse, null));
        }

        private static void DisposeBestEffort(IDisposable? disposable)
        {
            try
            {
                disposable?.Dispose();
            }
            catch
            {
                // The operation outcome owns the result; cleanup must never replace or classify it.
            }
        }
    }

    private sealed class ResponseObserverException(Exception innerException)
        : Exception("The response observer failed.", innerException);

    private sealed class CircuitTimeProvider : TimeProvider
    {
        private readonly object _gate = new();
        private readonly TimeProvider _inner;
        private readonly TimeScale _samplingScale;
        private readonly TimeScale _openScale;
        private long _actualAnchorTimestamp;
        private long _virtualAnchorTicks;
        private long _utcActualAnchorTimestamp;
        private DateTimeOffset _virtualUtcAnchor;
        private TimeScale _currentScale;

        private CircuitTimeProvider(
            TimeProvider inner,
            TimeSpan samplingDuration,
            TimeSpan breakDuration)
        {
            _inner = inner;
            PollySamplingDuration = ClampForPolly(samplingDuration);
            PollyBreakDuration = ClampForPolly(breakDuration);
            _samplingScale = new TimeScale(PollySamplingDuration.Ticks, samplingDuration.Ticks);
            _openScale = new TimeScale(PollyBreakDuration.Ticks, breakDuration.Ticks);
            _currentScale = _samplingScale;
            _actualAnchorTimestamp = inner.GetTimestamp();
            _utcActualAnchorTimestamp = _actualAnchorTimestamp;
            _virtualUtcAnchor = CircuitUtcEpoch;
        }

        public override TimeZoneInfo LocalTimeZone => _inner.LocalTimeZone;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public TimeSpan PollySamplingDuration { get; }

        public TimeSpan PollyBreakDuration { get; }

        public static CircuitTimeProvider Create(
            TimeProvider inner,
            TimeSpan samplingDuration,
            TimeSpan breakDuration)
            => new(inner, samplingDuration, breakDuration);

        public void EnterOpenState()
        {
            lock (_gate)
            {
                SwitchScale(_openScale);
            }
        }

        public void EnterSamplingState()
        {
            lock (_gate)
            {
                SwitchScale(_samplingScale);
            }
        }

        public override DateTimeOffset GetUtcNow()
        {
            lock (_gate)
            {
                return GetUtcNowCore();
            }
        }

        public override long GetTimestamp()
        {
            lock (_gate)
            {
                return GetTimestampCore();
            }
        }

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period)
            => _inner.CreateTimer(
                callback,
                state,
                ToCallerDuration(dueTime),
                ToCallerDuration(period));

        private static TimeSpan ClampForPolly(TimeSpan duration)
            => duration < PollyMinimumCircuitDuration
                ? PollyMinimumCircuitDuration
                : duration > PollyMaximumCircuitDuration
                    ? PollyMaximumCircuitDuration
                    : duration;

        private long GetTimestampCore()
        {
            var actualTimestamp = _inner.GetTimestamp();
            var actualElapsed = _inner.GetElapsedTime(_actualAnchorTimestamp, actualTimestamp);
            var scaledTicks = _currentScale.ToPollyTicks(actualElapsed.Ticks, MaximumRelevantCircuitElapsedTicks);
            _actualAnchorTimestamp = actualTimestamp;
            _virtualAnchorTicks = unchecked(_virtualAnchorTicks + scaledTicks);
            return _virtualAnchorTicks;
        }

        private void SwitchScale(TimeScale scale)
        {
            var timestamp = GetTimestampCore();
            var utcNow = GetUtcNowCore();
            _virtualAnchorTicks = timestamp;
            _virtualUtcAnchor = utcNow;
            _currentScale = scale;
        }

        private DateTimeOffset GetUtcNowCore()
        {
            var actualTimestamp = _inner.GetTimestamp();
            var actualElapsed = _inner.GetElapsedTime(_utcActualAnchorTimestamp, actualTimestamp);
            var scaledTicks = _currentScale.ToPollyTicks(actualElapsed.Ticks, MaximumRelevantCircuitElapsedTicks);
            _utcActualAnchorTimestamp = actualTimestamp;
            _virtualUtcAnchor += TimeSpan.FromTicks(scaledTicks);
            return _virtualUtcAnchor;
        }

        private TimeSpan ToCallerDuration(TimeSpan duration)
        {
            if (duration == Timeout.InfiniteTimeSpan)
            {
                return duration;
            }

            TimeScale currentScale;
            lock (_gate)
            {
                currentScale = _currentScale;
            }

            var unscaledTicks = currentScale.ToCallerTicks(duration.Ticks);
            return duration > TimeSpan.Zero && unscaledTicks < 1
                ? TimeSpan.FromTicks(1)
                : TimeSpan.FromTicks(unscaledTicks);
        }

        private readonly record struct TimeScale(long PollyTicks, long CallerTicks)
        {
            public long ToPollyTicks(long callerTicks, long maximumTicks)
            {
                var ticks = (BigInteger)callerTicks * PollyTicks / CallerTicks;
                return ticks >= maximumTicks ? maximumTicks : (long)ticks;
            }

            public long ToCallerTicks(long pollyTicks)
            {
                var dividend = (BigInteger)pollyTicks * CallerTicks;
                var ticks = BigInteger.DivRem(dividend, PollyTicks, out var remainder);
                if (!remainder.IsZero)
                {
                    ticks++;
                }

                return ticks >= TimeSpan.MaxValue.Ticks ? TimeSpan.MaxValue.Ticks : (long)ticks;
            }
        }
    }

    private static void Validate(HttpRequestResilienceOptions options)
    {
        ValidateMinimum(options.MaxAttempts, 1, nameof(options.MaxAttempts));

        ValidatePositive(options.TotalRequestTimeout, nameof(options.TotalRequestTimeout));
        ValidatePositive(options.BackoffBaseDelay, nameof(options.BackoffBaseDelay));
        ValidatePositive(options.MaximumDelay, nameof(options.MaximumDelay));
        ValidatePositive(options.CircuitSamplingDuration, nameof(options.CircuitSamplingDuration));
        ValidatePositive(options.CircuitBreakDuration, nameof(options.CircuitBreakDuration));

        ValidateMaximum(options.BackoffBaseDelay, options.MaximumDelay, nameof(options.BackoffBaseDelay));
        ValidateFailureRatio(options.CircuitFailureRatio, nameof(options.CircuitFailureRatio));
        ValidateMinimum(options.CircuitMinimumThroughput, 2, nameof(options.CircuitMinimumThroughput));

        ArgumentNullException.ThrowIfNull(options.TimeProvider);
        ArgumentNullException.ThrowIfNull(options.JitterProvider);
        ArgumentNullException.ThrowIfNull(options.RetryableStatusCodes);
        ArgumentNullException.ThrowIfNull(options.RetryableExceptionCategories);

        ValidateRetryableExceptionCategories(options.RetryableExceptionCategories);
    }

    private static void ValidateRetryableExceptionCategories(
        IReadOnlySet<HttpResilienceFailureCategory> retryableExceptionCategories)
    {
        foreach (var category in retryableExceptionCategories)
        {
            if (category is not (HttpResilienceFailureCategory.Transport or HttpResilienceFailureCategory.Timeout))
            {
                throw new ArgumentOutOfRangeException(nameof(retryableExceptionCategories));
            }
        }
    }

    private static void ValidatePositive(TimeSpan value, string parameterName)
    {
        if (value <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(parameterName);
        }
    }

    private static void ValidateMinimum(int value, int minimum, string parameterName)
    {
        if (value < minimum)
        {
            throw new ArgumentOutOfRangeException(parameterName);
        }
    }

    private static void ValidateMaximum(TimeSpan value, TimeSpan maximum, string parameterName)
    {
        if (value > maximum)
        {
            throw new ArgumentOutOfRangeException(parameterName);
        }
    }

    private static void ValidateFailureRatio(double value, string parameterName)
    {
        if (value is not (> 0 and <= 1))
        {
            throw new ArgumentOutOfRangeException(parameterName);
        }
    }
}
