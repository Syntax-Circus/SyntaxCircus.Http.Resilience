using System.Net;
using Microsoft.Extensions.Http.Resilience;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;

namespace SyntaxCircus.Http.Resilience;

public static class ResilientHttpClientExtensions
{
    /// <summary>
    /// Registers a named <see cref="HttpClient"/> with retry + circuit-breaker resilience
    /// (exponential backoff with jitter, retrying transport/timeouts and the shared transient HTTP status set).
    /// <paramref name="aiMode"/> excludes 429 (rate-limited) from retry/circuit-breaking — useful
    /// for AI/LLM provider clients, where a 429 means "back off on purpose", not "something's broken".
    /// </summary>
    /// <param name="onRetry">
    /// Optional callback invoked on each retry attempt, receiving the client <paramref name="name"/>,
    /// the 1-based attempt number, and the response status code (if any — <see langword="null"/> for
    /// exception-driven retries). Useful for wiring retry telemetry/diagnostics without having to
    /// hand-roll the underlying Polly pipeline yourself.
    /// </param>
    /// <param name="onBreak">
    /// Optional callback invoked when the circuit breaker opens, receiving the client
    /// <paramref name="name"/> and the triggering status code (if any).
    /// </param>
    public static IHttpClientBuilder AddResilientHttpClient(
        this IServiceCollection services,
        string name,
        Action<HttpClient>? configureClient = null,
        int retryCount = 2,
        bool aiMode = false,
        Action<string, int, HttpStatusCode?>? onRetry = null,
        Action<string, HttpStatusCode?>? onBreak = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var builder = configureClient is null
            ? services.AddHttpClient(name)
            : services.AddHttpClient(name, configureClient);

        builder.AddResilienceHandler($"{name}-resilience", pipelineBuilder =>
        {
            pipelineBuilder.AddRetry(new RetryStrategyOptions<HttpResponseMessage>
            {
                MaxRetryAttempts = retryCount,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = args => ValueTask.FromResult(HttpResilienceOutcomeClassifier.ShouldHandle(
                    args.Outcome,
                    includeTooManyRequests: !aiMode,
                    args.Context.CancellationToken)),
                OnRetry = args =>
                {
                    onRetry?.Invoke(name, args.AttemptNumber + 1, args.Outcome.Result?.StatusCode);
                    return ValueTask.CompletedTask;
                },
            });

            pipelineBuilder.AddCircuitBreaker(new CircuitBreakerStrategyOptions<HttpResponseMessage>
            {
                FailureRatio = 0.5,
                MinimumThroughput = 5,
                BreakDuration = TimeSpan.FromSeconds(30),
                ShouldHandle = args => ValueTask.FromResult(HttpResilienceOutcomeClassifier.ShouldHandle(
                    args.Outcome,
                    includeTooManyRequests: !aiMode,
                    args.Context.CancellationToken)),
                OnOpened = args =>
                {
                    onBreak?.Invoke(name, args.Outcome.Result?.StatusCode);
                    return ValueTask.CompletedTask;
                },
            });
        });

        return builder;
    }
}

internal static class HttpResilienceOutcomeClassifier
{
    private static readonly IReadOnlySet<HttpStatusCode> RetryableStatusCodesWithoutTooManyRequests =
        HttpRequestResilienceOptions.DefaultRetryableStatusCodes
            .Where(statusCode => statusCode != HttpStatusCode.TooManyRequests)
            .ToHashSet();

    public static bool ShouldHandle(
        Outcome<HttpResponseMessage> outcome,
        bool includeTooManyRequests,
        CancellationToken cancellationToken)
        => TryClassify(
            outcome.Result,
            outcome.Exception,
            includeTooManyRequests
                ? HttpRequestResilienceOptions.DefaultRetryableStatusCodes
                : RetryableStatusCodesWithoutTooManyRequests,
            HttpRequestResilienceOptions.DefaultRetryableExceptionCategories,
            cancellationToken,
            out _);

    public static bool TryClassify(
        HttpResponseMessage? response,
        Exception? exception,
        IReadOnlySet<HttpStatusCode> retryableStatusCodes,
        IReadOnlySet<HttpResilienceFailureCategory> retryableExceptionCategories,
        CancellationToken cancellationToken,
        out HttpResilienceFailureCategory category)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            category = HttpResilienceFailureCategory.Timeout;
            return false;
        }

        if (exception is HttpRequestException)
        {
            category = HttpResilienceFailureCategory.Transport;
            return retryableExceptionCategories.Contains(category);
        }

        if (exception is TimeoutException
            || exception is OperationCanceledException && !cancellationToken.IsCancellationRequested)
        {
            category = HttpResilienceFailureCategory.Timeout;
            return retryableExceptionCategories.Contains(category);
        }

        category = HttpResilienceFailureCategory.HttpStatus;
        return response is not null && retryableStatusCodes.Contains(response.StatusCode);
    }
}
