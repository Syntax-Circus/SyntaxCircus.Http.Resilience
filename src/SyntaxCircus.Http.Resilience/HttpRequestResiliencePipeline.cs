using System.Collections.Frozen;
using System.Net;
using System.Net.Http;
using System.Runtime.ExceptionServices;
using Polly;
using Polly.Retry;

namespace SyntaxCircus.Http.Resilience;

public sealed class HttpRequestResiliencePipeline
{
    private static readonly ResiliencePropertyKey<ExecutionState> ExecutionStateKey = new("HttpRequestExecutionState");

    private readonly string _name;
    private readonly HttpRequestResilienceOptions _options;
    private readonly ResiliencePipeline<AttemptResult> _retryPipeline;
    private readonly LogicalCircuitBreaker _circuitBreaker;

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

        _circuitBreaker = new LogicalCircuitBreaker(_name, _options);
        _retryPipeline = BuildRetryPipeline();
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
                ShouldHandle = args =>
                {
                    var state = GetExecutionState(args.Context);
                    return ValueTask.FromResult(
                        state.ReplaySafety == HttpRequestReplaySafety.Replayable
                        && !state.Budget.HasTerminalOutcome
                        && TryClassify(args.Outcome, args.Context.CancellationToken, out _));
                },
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
                    GetExecutionState(args.Context).Budget.ThrowIfTerminal();
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
        cancellationToken.ThrowIfCancellationRequested();
        using var budget = new RequestBudget(_options.TimeProvider, _options.TotalRequestTimeout, cancellationToken);
        budget.ThrowIfTerminal();

        var circuitEntry = _circuitBreaker.TryEnter();
        if (cancellationToken.IsCancellationRequested)
        {
            _circuitBreaker.Exclude(circuitEntry);
            ThrowCallerCancellation(cancellationToken);
        }
        if (!circuitEntry.IsAdmitted)
        {
            budget.ThrowIfTerminal();
            throw new HttpCircuitOpenException(_name, circuitEntry.RetryAfter);
        }

        await InvokeCircuitCallbackSafelyAsync(circuitEntry.Transition, cancellationToken).ConfigureAwait(false);

        var state = new ExecutionState(
            requestFactory,
            sender,
            completionOption,
            replaySafety,
            responseObserver,
            budget);
        ResilienceContext? context = null;

        try
        {
            budget.ThrowIfTerminal();
            context = ResilienceContextPool.Shared.Get(budget.ExecutionToken);
            context.Properties.Set(ExecutionStateKey, state);

            AttemptResult result;
            try
            {
                result = await _retryPipeline.ExecuteAsync(
                    static (executionContext, executionState) => executionState.SendAttemptAsync(executionContext.CancellationToken),
                    context,
                    state).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                state.DisposePendingResponseBestEffort();
                return await MapTerminalExceptionAsync(exception, circuitEntry, budget, cancellationToken).ConfigureAwait(false);
            }

            try
            {
                budget.ThrowIfTerminal();
            }
            catch (Exception exception)
            {
                result.DisposeForTerminal();
                return await MapTerminalExceptionAsync(exception, circuitEntry, budget, cancellationToken).ConfigureAwait(false);
            }

            var completion = _circuitBreaker.Complete(
                circuitEntry,
                ClassifyCircuitOutcome(result.Response, exception: null),
                cancellationToken);
            if (completion.CancellationWon)
            {
                result.DisposeForTerminal();
                ThrowCallerCancellation(cancellationToken);
            }

            await InvokeCircuitCallbackSafelyAsync(completion.Transition, cancellationToken).ConfigureAwait(false);
            state.ReleaseResponse(result.Response);
            return result.Response;
        }
        catch (Exception exception) when (context is null)
        {
            return await MapTerminalExceptionAsync(exception, circuitEntry, budget, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (context is not null)
            {
                ResilienceContextPool.Shared.Return(context);
            }
        }
    }

    private async Task<HttpResponseMessage> MapTerminalExceptionAsync(
        Exception exception,
        CircuitEntry circuitEntry,
        RequestBudget budget,
        CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            _circuitBreaker.Exclude(circuitEntry);
            ThrowCallerCancellation(cancellationToken, exception);
        }

        if (budget.IsExpired)
        {
            return await ThrowLogicalTimeoutAsync(circuitEntry, exception, cancellationToken).ConfigureAwait(false);
        }

        if (exception is ResponseObserverException observerException)
        {
            _circuitBreaker.Exclude(circuitEntry);
            ExceptionDispatchInfo.Capture(observerException.InnerException!).Throw();
        }

        var completion = _circuitBreaker.Complete(
            circuitEntry,
            ClassifyCircuitOutcome(response: null, exception),
            cancellationToken);
        if (completion.CancellationWon)
        {
            ThrowCallerCancellation(cancellationToken, exception);
        }

        await InvokeCircuitCallbackSafelyAsync(completion.Transition, cancellationToken).ConfigureAwait(false);
        ExceptionDispatchInfo.Capture(exception).Throw();
        throw new InvalidOperationException("Unreachable exception mapping path.");
    }

    private async Task<HttpResponseMessage> ThrowLogicalTimeoutAsync(
        CircuitEntry circuitEntry,
        Exception innerException,
        CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            _circuitBreaker.Exclude(circuitEntry);
            ThrowCallerCancellation(cancellationToken, innerException);
        }

        await InvokeTimeoutCallbackSafelyAsync(
            new HttpTimeoutTelemetry(
                _name,
                HttpResilienceFailureCategory.Timeout,
                _options.TotalRequestTimeout),
            cancellationToken).ConfigureAwait(false);

        var timeoutOutcome = ClassifyCircuitOutcome(
            response: null,
            new LogicalTimeoutException(innerException));
        var completion = _circuitBreaker.Complete(
            circuitEntry,
            timeoutOutcome,
            cancellationToken);
        if (completion.CancellationWon)
        {
            ThrowCallerCancellation(cancellationToken, innerException);
        }

        await InvokeCircuitCallbackSafelyAsync(completion.Transition, cancellationToken).ConfigureAwait(false);

        throw new HttpRequestTimeoutException(_name, _options.TotalRequestTimeout, innerException);
    }

    private TimeSpan GetRetryDelay(RetryDelayGeneratorArguments<AttemptResult> args)
    {
        var state = GetExecutionState(args.Context);
        var remaining = state.Budget.GetPositiveRemainingOrThrow();
        var delay = TryGetRetryAfter(args.Outcome.Result?.Response, out var retryAfter)
            ? retryAfter
            : GetBackoffDelay(args.AttemptNumber);

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
            : ClassifyForTelemetry(outcome.Result?.Response, outcome.Exception);

    private CircuitOutcome ClassifyCircuitOutcome(HttpResponseMessage? response, Exception? exception)
    {
        var isFailure = HttpResilienceOutcomeClassifier.TryClassify(
            response,
            exception,
            _options.RetryableStatusCodes,
            _options.RetryableExceptionCategories,
            CancellationToken.None,
            out var category);
        return new CircuitOutcome(
            isFailure,
            response?.StatusCode,
            isFailure ? category : ClassifyForTelemetry(response, exception));
    }

    private static HttpResilienceFailureCategory ClassifyForTelemetry(
        HttpResponseMessage? response,
        Exception? exception)
        => exception switch
        {
            HttpRequestException => HttpResilienceFailureCategory.Transport,
            TimeoutException or OperationCanceledException => HttpResilienceFailureCategory.Timeout,
            _ when response is not null => HttpResilienceFailureCategory.HttpStatus,
            _ => HttpResilienceFailureCategory.Transport,
        };

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
        HttpCircuitTelemetry? telemetry,
        CancellationToken cancellationToken)
    {
        if (telemetry is null || _options.OnCircuitStateChanged is null)
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

    private static void ThrowCallerCancellation(CancellationToken cancellationToken, Exception? innerException = null)
        => throw new OperationCanceledException("The operation was canceled.", innerException, cancellationToken);

    private sealed class AttemptResult(HttpResponseMessage response, ExecutionState state)
    {
        public HttpResponseMessage Response { get; } = response;

        public void DisposeForRetry()
        {
            DisposeBestEffort(Response);
            state.ReleaseResponse(Response);
        }

        public void DisposeForTerminal()
        {
            DisposeBestEffort(Response);
            state.ReleaseResponse(Response);
        }
    }

    private sealed class ExecutionState(
        Func<int, CancellationToken, ValueTask<HttpRequestMessage>> requestFactory,
        Func<HttpRequestMessage, HttpCompletionOption, CancellationToken, Task<HttpResponseMessage>> sender,
        HttpCompletionOption completionOption,
        HttpRequestReplaySafety replaySafety,
        Func<HttpResponseMessage, CancellationToken, ValueTask>? responseObserver,
        RequestBudget budget)
    {
        private HttpResponseMessage? _pendingResponse;
        private int _attemptNumber;

        public HttpRequestReplaySafety ReplaySafety { get; } = replaySafety;

        public RequestBudget Budget { get; } = budget;

        public async ValueTask<AttemptResult> SendAttemptAsync(CancellationToken cancellationToken)
        {
            Budget.ThrowIfTerminal();
            var attemptNumber = Interlocked.Increment(ref _attemptNumber);
            HttpRequestMessage? request = null;
            HttpResponseMessage? response = null;

            try
            {
                request = await CreateRequestAsync(attemptNumber, cancellationToken).ConfigureAwait(false);
                Budget.ThrowIfTerminal();

                Task<HttpResponseMessage> senderTask;
                try
                {
                    var scheduledRequest = request;
                    senderTask = Task.Run(
                        async () =>
                        {
                            var work = sender(scheduledRequest, completionOption, cancellationToken)
                                ?? throw new InvalidOperationException("The sender returned null task.");
                            return await work.ConfigureAwait(false)
                                ?? throw new InvalidOperationException("The sender returned null.");
                        },
                        CancellationToken.None);
                }
                catch (Exception exception)
                {
                    Budget.ThrowIfTerminal(exception);
                    throw;
                }

                if (!senderTask.IsCompleted)
                {
                    var completed = await Task.WhenAny(senderTask, Budget.SignalTask).ConfigureAwait(false);
                    if (completed != senderTask)
                    {
                        ScheduleLateResponseObservation(senderTask, request);
                        request = null;
                        Budget.ThrowIfTerminal();
                    }
                }

                try
                {
                    response = await senderTask.ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    Budget.ThrowIfTerminal(exception);
                    throw;
                }

                if (Budget.HasTerminalOutcome)
                {
                    ScheduleLateResponseObservation(Task.FromResult(response), request!);
                    response = null;
                    request = null;
                    Budget.ThrowIfTerminal();
                }

                if (responseObserver is not null)
                {
                    Task observerTask;
                    try
                    {
                        var scheduledResponse = response!;
                        observerTask = Task.Run(
                            async () => await responseObserver(scheduledResponse, cancellationToken).ConfigureAwait(false),
                            CancellationToken.None);
                    }
                    catch (Exception exception)
                    {
                        Budget.ThrowIfTerminal(exception);
                        throw new ResponseObserverException(exception);
                    }

                    if (!observerTask.IsCompleted)
                    {
                        var completed = await Task.WhenAny(observerTask, Budget.SignalTask).ConfigureAwait(false);
                        if (completed != observerTask)
                        {
                            _ = ObserveLateObserverAsync(observerTask, response!, request!);
                            response = null;
                            request = null;
                            Budget.ThrowIfTerminal();
                        }
                    }

                    try
                    {
                        await observerTask.ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        Budget.ThrowIfTerminal(exception);
                        throw new ResponseObserverException(exception);
                    }
                }

                Budget.ThrowIfTerminal();
                DisposeBestEffort(request);
                request = null;
                Volatile.Write(ref _pendingResponse, response);
                var result = new AttemptResult(response!, this);
                response = null;
                return result;
            }
            finally
            {
                DisposeBestEffort(response);
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

        private async ValueTask<HttpRequestMessage> CreateRequestAsync(
            int attemptNumber,
            CancellationToken cancellationToken)
        {
            Task<HttpRequestMessage> factoryTask;
            try
            {
                factoryTask = Task.Run(
                    async () => await requestFactory(attemptNumber, cancellationToken).ConfigureAwait(false)
                        ?? throw new InvalidOperationException("The request factory returned null."),
                    CancellationToken.None);
            }
            catch (Exception exception)
            {
                Budget.ThrowIfTerminal(exception);
                throw;
            }

            if (!factoryTask.IsCompleted)
            {
                var completed = await Task.WhenAny(factoryTask, Budget.SignalTask).ConfigureAwait(false);
                if (completed != factoryTask)
                {
                    _ = ObserveLateFactoryAsync(factoryTask);
                    Budget.ThrowIfTerminal();
                }

                try
                {
                    var request = await factoryTask.ConfigureAwait(false)
                        ?? throw new InvalidOperationException("The request factory returned null.");
                    if (Budget.HasTerminalOutcome)
                    {
                        DisposeBestEffort(request);
                        Budget.ThrowIfTerminal();
                    }

                    return request;
                }
                catch (Exception exception)
                {
                    Budget.ThrowIfTerminal(exception);
                    throw;
                }
            }

            try
            {
                var request = await factoryTask.ConfigureAwait(false)
                    ?? throw new InvalidOperationException("The request factory returned null.");
                if (Budget.HasTerminalOutcome)
                {
                    DisposeBestEffort(request);
                    Budget.ThrowIfTerminal();
                }

                return request;
            }
            catch (Exception exception)
            {
                Budget.ThrowIfTerminal(exception);
                throw;
            }
        }

        private static async Task ObserveLateFactoryAsync(Task<HttpRequestMessage> factoryTask)
        {
            try
            {
                DisposeBestEffort(await factoryTask.ConfigureAwait(false));
            }
            catch
            {
            }
        }

        private void ScheduleLateResponseObservation(
            Task<HttpResponseMessage> senderTask,
            HttpRequestMessage request)
        {
            _ = Task.Run(
                async () =>
                {
                    HttpResponseMessage? response = null;
                    try
                    {
                        response = await senderTask.ConfigureAwait(false);
                        if (response is not null && responseObserver is not null)
                        {
                            try
                            {
                                // The sender await may resume inline on its completion thread.
                                // Queue observer work before any user code can run on that thread.
                                var scheduledResponse = response;
                                await Task.Run(
                                    async () => await responseObserver(
                                        scheduledResponse,
                                        Budget.ExecutionToken).ConfigureAwait(false),
                                    CancellationToken.None).ConfigureAwait(false);
                            }
                            catch
                            {
                            }
                        }
                    }
                    catch
                    {
                    }
                    finally
                    {
                        DisposeBestEffort(response);
                        DisposeBestEffort(request);
                    }
                },
                CancellationToken.None);
        }

        private static async Task ObserveLateObserverAsync(
            Task observerTask,
            HttpResponseMessage response,
            HttpRequestMessage request)
        {
            try
            {
                await observerTask.ConfigureAwait(false);
            }
            catch
            {
            }
            finally
            {
                DisposeBestEffort(response);
                DisposeBestEffort(request);
            }
        }
    }

    private sealed class RequestBudget : IDisposable
    {
        private static readonly TimeSpan MaximumTimerSegment = TimeSpan.FromDays(24);

        private readonly TimeProvider _timeProvider;
        private readonly TimeSpan _timeout;
        private readonly CancellationToken _callerCancellationToken;
        private readonly long _startTimestamp;
        private readonly CancellationTokenSource? _deadlineSource;
        private readonly CancellationTokenSource? _linkedSource;
        private readonly TaskCompletionSource _signal = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly CancellationTokenRegistration _signalRegistration;
        private ITimer? _timer;
        private int _disposed;
        private int _expired;

        public RequestBudget(
            TimeProvider timeProvider,
            TimeSpan timeout,
            CancellationToken callerCancellationToken)
        {
            _timeProvider = timeProvider;
            _timeout = timeout;
            _callerCancellationToken = callerCancellationToken;
            _startTimestamp = timeProvider.GetTimestamp();

            if (timeout == TimeSpan.MaxValue)
            {
                ExecutionToken = callerCancellationToken;
                _signalRegistration = callerCancellationToken.Register(
                    static state => ((TaskCompletionSource)state!).TrySetResult(),
                    _signal);
                return;
            }

            _deadlineSource = new CancellationTokenSource();
            _linkedSource = CancellationTokenSource.CreateLinkedTokenSource(
                callerCancellationToken,
                _deadlineSource.Token);
            ExecutionToken = _linkedSource.Token;
            _signalRegistration = ExecutionToken.Register(
                static state => ((TaskCompletionSource)state!).TrySetResult(),
                _signal);
            _timer = timeProvider.CreateTimer(
                static state => ((RequestBudget)state!).OnTimer(),
                this,
                GetTimerSegment(timeout),
                Timeout.InfiniteTimeSpan);
        }

        public CancellationToken ExecutionToken { get; }

        public Task SignalTask => _signal.Task;

        public bool HasTerminalOutcome => _callerCancellationToken.IsCancellationRequested || IsExpired;

        public bool IsExpired
        {
            get
            {
                if (_timeout == TimeSpan.MaxValue)
                {
                    return false;
                }

                if (Volatile.Read(ref _expired) != 0)
                {
                    return true;
                }

                if (GetRemaining() > TimeSpan.Zero)
                {
                    return false;
                }

                Expire();
                return true;
            }
        }

        public TimeSpan GetPositiveRemainingOrThrow()
        {
            ThrowIfTerminal();
            var remaining = GetRemaining();
            if (remaining <= TimeSpan.Zero)
            {
                Expire();
                throw new LogicalTimeoutException();
            }

            return remaining;
        }

        public void ThrowIfTerminal(Exception? innerException = null)
        {
            if (_callerCancellationToken.IsCancellationRequested)
            {
                ThrowCallerCancellation(_callerCancellationToken, innerException);
            }

            if (IsExpired)
            {
                throw new LogicalTimeoutException(innerException);
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _timer?.Dispose();
            _signalRegistration.Dispose();
            _linkedSource?.Dispose();
            _deadlineSource?.Dispose();
        }

        private void OnTimer()
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                return;
            }

            var remaining = GetRemaining();
            if (remaining <= TimeSpan.Zero)
            {
                Expire();
                return;
            }

            try
            {
                _timer?.Change(GetTimerSegment(remaining), Timeout.InfiniteTimeSpan);
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private TimeSpan GetRemaining()
        {
            if (_timeout == TimeSpan.MaxValue)
            {
                return TimeSpan.MaxValue;
            }

            var elapsed = _timeProvider.GetElapsedTime(_startTimestamp);
            return elapsed >= _timeout ? TimeSpan.Zero : _timeout - elapsed;
        }

        private void Expire()
        {
            if (Interlocked.Exchange(ref _expired, 1) != 0)
            {
                return;
            }

            try
            {
                _deadlineSource?.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private static TimeSpan GetTimerSegment(TimeSpan remaining)
            => remaining <= MaximumTimerSegment ? remaining : MaximumTimerSegment;
    }

    private sealed class LogicalCircuitBreaker
    {
        private readonly object _gate = new();
        private readonly string _name;
        private readonly HttpRequestResilienceOptions _options;
        private readonly Queue<CircuitSample> _samples = [];
        private CircuitState _state;
        private long _generation;
        private long _openedTimestamp;
        private bool _halfOpenProbeActive;
        private int _failureCount;

        public LogicalCircuitBreaker(string name, HttpRequestResilienceOptions options)
        {
            _name = name;
            _options = options;
        }

        public CircuitEntry TryEnter()
        {
            lock (_gate)
            {
                var now = _options.TimeProvider.GetTimestamp();
                if (_state == CircuitState.Closed)
                {
                    return CircuitEntry.AdmittedClosed(_generation);
                }

                if (_state == CircuitState.Open)
                {
                    var elapsed = _options.TimeProvider.GetElapsedTime(_openedTimestamp, now);
                    if (elapsed < _options.CircuitBreakDuration)
                    {
                        return CircuitEntry.Rejected(_options.CircuitBreakDuration - elapsed);
                    }

                    _state = CircuitState.HalfOpen;
                    _generation++;
                    _halfOpenProbeActive = true;
                    return CircuitEntry.AdmittedProbe(
                        _generation,
                        new HttpCircuitTelemetry(
                            _name,
                            HttpResilienceCircuitState.HalfOpen,
                            null,
                            HttpResilienceFailureCategory.CircuitOpen));
                }

                if (_halfOpenProbeActive)
                {
                    return CircuitEntry.Rejected(TimeSpan.Zero);
                }

                _halfOpenProbeActive = true;
                return CircuitEntry.AdmittedProbe(_generation, transition: null);
            }
        }

        public CircuitCompletion Complete(
            CircuitEntry entry,
            CircuitOutcome outcome,
            CancellationToken cancellationToken)
        {
            if (!entry.IsAdmitted)
            {
                return CircuitCompletion.Committed(transition: null);
            }

            lock (_gate)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    ExcludeCore(entry);
                    return CircuitCompletion.Canceled;
                }

                if (entry.IsProbe)
                {
                    if (_state != CircuitState.HalfOpen || entry.Generation != _generation)
                    {
                        return CircuitCompletion.Committed(transition: null);
                    }

                    if (outcome.IsFailure)
                    {
                        var probeTimestamp = _options.TimeProvider.GetTimestamp();
                        // The provider is injected and may cancel the caller while producing the timestamp.
                        // This check is the terminal linearization point: cancellation before it excludes
                        // the outcome; cancellation after the committed mutation is post-terminal.
                        if (cancellationToken.IsCancellationRequested)
                        {
                            ExcludeCore(entry);
                            return CircuitCompletion.Canceled;
                        }

                        _halfOpenProbeActive = false;
                        return CircuitCompletion.Committed(OpenCircuit(probeTimestamp, outcome));
                    }

                    _halfOpenProbeActive = false;
                    _state = CircuitState.Closed;
                    _generation++;
                    _samples.Clear();
                    _failureCount = 0;
                    return CircuitCompletion.Committed(new HttpCircuitTelemetry(
                        _name,
                        HttpResilienceCircuitState.Closed,
                        outcome.StatusCode,
                        outcome.Category));
                }

                if (_state != CircuitState.Closed || entry.Generation != _generation)
                {
                    return CircuitCompletion.Committed(transition: null);
                }

                var now = _options.TimeProvider.GetTimestamp();
                // Keep cancellation selection and sample/state mutation in the same critical section.
                // No callback runs under this lock, so a post-commit callback cannot require rollback.
                if (cancellationToken.IsCancellationRequested)
                {
                    return CircuitCompletion.Canceled;
                }

                Prune(now);
                _samples.Enqueue(new CircuitSample(now, outcome.IsFailure));
                if (outcome.IsFailure)
                {
                    _failureCount++;
                }

                if (!outcome.IsFailure
                    || _samples.Count < _options.CircuitMinimumThroughput
                    || (double)_failureCount / _samples.Count < _options.CircuitFailureRatio)
                {
                    return CircuitCompletion.Committed(transition: null);
                }

                return CircuitCompletion.Committed(OpenCircuit(now, outcome));
            }
        }

        public void Exclude(CircuitEntry entry)
        {
            if (!entry.IsAdmitted || !entry.IsProbe)
            {
                return;
            }

            lock (_gate)
            {
                ExcludeCore(entry);
            }
        }

        private void ExcludeCore(CircuitEntry entry)
        {
            if (entry.IsProbe && _state == CircuitState.HalfOpen && entry.Generation == _generation)
            {
                _halfOpenProbeActive = false;
            }
        }

        private HttpCircuitTelemetry OpenCircuit(long timestamp, CircuitOutcome outcome)
        {
            _state = CircuitState.Open;
            _generation++;
            _openedTimestamp = timestamp;
            _halfOpenProbeActive = false;
            _samples.Clear();
            _failureCount = 0;
            return new HttpCircuitTelemetry(
                _name,
                HttpResilienceCircuitState.Open,
                outcome.StatusCode,
                outcome.Category);
        }

        private void Prune(long now)
        {
            while (_samples.TryPeek(out var sample)
                && _options.TimeProvider.GetElapsedTime(sample.Timestamp, now) >= _options.CircuitSamplingDuration)
            {
                _samples.Dequeue();
                if (sample.IsFailure)
                {
                    _failureCount--;
                }
            }
        }

        private enum CircuitState
        {
            Closed,
            Open,
            HalfOpen,
        }
    }

    private readonly record struct CircuitEntry(
        bool IsAdmitted,
        bool IsProbe,
        long Generation,
        TimeSpan RetryAfter,
        HttpCircuitTelemetry? Transition)
    {
        public static CircuitEntry AdmittedClosed(long generation)
            => new(true, false, generation, default, null);

        public static CircuitEntry AdmittedProbe(long generation, HttpCircuitTelemetry? transition)
            => new(true, true, generation, default, transition);

        public static CircuitEntry Rejected(TimeSpan retryAfter)
            => new(false, false, default, retryAfter < TimeSpan.Zero ? TimeSpan.Zero : retryAfter, null);
    }

    private readonly record struct CircuitOutcome(
        bool IsFailure,
        HttpStatusCode? StatusCode,
        HttpResilienceFailureCategory Category);

    private readonly record struct CircuitCompletion(
        bool CancellationWon,
        HttpCircuitTelemetry? Transition)
    {
        public static CircuitCompletion Canceled => new(true, null);

        public static CircuitCompletion Committed(HttpCircuitTelemetry? transition)
            => new(false, transition);
    }

    private readonly record struct CircuitSample(long Timestamp, bool IsFailure);

    private sealed class ResponseObserverException(Exception innerException)
        : Exception("The response observer failed.", innerException);

    private sealed class LogicalTimeoutException(Exception? innerException = null)
        : TimeoutException("The logical request budget expired.", innerException);

    private static void DisposeBestEffort(IDisposable? disposable)
    {
        try
        {
            disposable?.Dispose();
        }
        catch
        {
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
