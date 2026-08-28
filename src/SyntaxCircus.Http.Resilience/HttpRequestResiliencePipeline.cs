using System.Net.Http;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;

namespace SyntaxCircus.Http.Resilience;

public sealed class HttpRequestResiliencePipeline
{
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
            OnRetry = options.OnRetry,
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
        var builder = new ResiliencePipelineBuilder<AttemptResult>
        {
            TimeProvider = _options.TimeProvider,
        };

        builder.AddCircuitBreaker(new CircuitBreakerStrategyOptions<AttemptResult>
        {
            FailureRatio = _options.CircuitFailureRatio,
            MinimumThroughput = _options.CircuitMinimumThroughput,
            SamplingDuration = Max(_options.CircuitSamplingDuration, TimeSpan.FromMilliseconds(500) + TimeSpan.FromTicks(1)),
            BreakDuration = Max(_options.CircuitBreakDuration, TimeSpan.FromMilliseconds(500) + TimeSpan.FromTicks(1)),
            BreakDurationGenerator = _ => ValueTask.FromResult(_options.CircuitBreakDuration),
            ShouldHandle = args => ValueTask.FromResult(TryClassify(args.Outcome, args.Context.CancellationToken, out _)),
            OnOpened = async args =>
            {
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
            OnHalfOpened = args => InvokeCircuitCallbackSafelyAsync(
                new HttpCircuitTelemetry(
                    _name,
                    HttpResilienceCircuitState.HalfOpen,
                    null,
                    HttpResilienceFailureCategory.CircuitOpen),
                args.Context.CancellationToken),
            OnClosed = args => InvokeCircuitCallbackSafelyAsync(
                new HttpCircuitTelemetry(
                    _name,
                    HttpResilienceCircuitState.Closed,
                    args.Outcome.Result?.Response.StatusCode,
                    ClassifyForTelemetry(args.Outcome, args.Context.CancellationToken)),
                args.Context.CancellationToken),
        });

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
            _options.TimeProvider.GetTimestamp());
        using var timeoutSource = new CancellationTokenSource(_options.TotalRequestTimeout, _options.TimeProvider);
        using var budgetSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutSource.Token);
        var context = ResilienceContextPool.Shared.Get(budgetSource.Token);
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
        catch (OperationCanceledException exception) when (cancellationToken.IsCancellationRequested)
        {
            state.DisposePendingResponse();
            throw new OperationCanceledException(exception.Message, exception, cancellationToken);
        }
        catch (OperationCanceledException exception) when (timeoutSource.IsCancellationRequested)
        {
            state.DisposePendingResponse();
            throw new HttpRequestTimeoutException(_name, _options.TotalRequestTimeout, exception);
        }
        catch (BrokenCircuitException exception)
        {
            state.DisposePendingResponse();
            throw new HttpCircuitOpenException(_name, GetCircuitRetryAfter(), exception);
        }
        catch
        {
            state.DisposePendingResponse();
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

    private static TimeSpan Max(TimeSpan first, TimeSpan second)
        => first >= second ? first : second;

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

    private static bool TryClassify(
        Outcome<AttemptResult> outcome,
        CancellationToken cancellationToken,
        out HttpResilienceFailureCategory category)
        => HttpResilienceOutcomeClassifier.TryClassify(
            outcome.Result?.Response,
            outcome.Exception,
            includeTooManyRequests: true,
            cancellationToken,
            out category);

    private static HttpResilienceFailureCategory ClassifyForTelemetry(
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

    private sealed class AttemptResult(HttpResponseMessage response, ExecutionState state)
    {
        public HttpResponseMessage Response { get; } = response;

        public void DisposeForRetry()
        {
            Response.Dispose();
            state.ReleaseResponse(Response);
        }
    }

    private sealed class ExecutionState(
        Func<int, CancellationToken, ValueTask<HttpRequestMessage>> requestFactory,
        Func<HttpRequestMessage, HttpCompletionOption, CancellationToken, Task<HttpResponseMessage>> sender,
        HttpCompletionOption completionOption,
        HttpRequestReplaySafety replaySafety,
        Func<HttpResponseMessage, CancellationToken, ValueTask>? responseObserver,
        long startTimestamp)
    {
        private HttpResponseMessage? _pendingResponse;
        private int _attemptNumber;

        public HttpRequestReplaySafety ReplaySafety { get; } = replaySafety;

        public long StartTimestamp { get; } = startTimestamp;

        public async ValueTask<AttemptResult> SendAttemptAsync(CancellationToken cancellationToken)
        {
            var attemptNumber = Interlocked.Increment(ref _attemptNumber);
            using var request = await requestFactory(attemptNumber, cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidOperationException("The request factory returned null.");
            var response = await sender(request, completionOption, cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidOperationException("The sender returned null.");
            Volatile.Write(ref _pendingResponse, response);

            if (responseObserver is not null)
            {
                await responseObserver(response, cancellationToken).ConfigureAwait(false);
            }

            return new AttemptResult(response, this);
        }

        public void ReleaseResponse(HttpResponseMessage response)
        {
            Interlocked.CompareExchange(ref _pendingResponse, null, response);
        }

        public void DisposePendingResponse()
        {
            Interlocked.Exchange(ref _pendingResponse, null)?.Dispose();
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
