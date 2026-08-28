using System.Net.Http;

namespace SyntaxCircus.Http.Resilience;

public sealed class HttpRequestResiliencePipeline
{
    private readonly HttpRequestResilienceOptions _options;

    public HttpRequestResiliencePipeline(string name, HttpRequestResilienceOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(options);

        Validate(options);

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
    }

    public Task<HttpResponseMessage> SendAsync(
        Func<int, CancellationToken, ValueTask<HttpRequestMessage>> requestFactory,
        Func<HttpRequestMessage, HttpCompletionOption, CancellationToken, Task<HttpResponseMessage>> sender,
        HttpCompletionOption completionOption,
        HttpRequestReplaySafety replaySafety,
        Func<HttpResponseMessage, CancellationToken, ValueTask>? responseObserver = null,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException("Request execution is implemented in Task 2.");

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
