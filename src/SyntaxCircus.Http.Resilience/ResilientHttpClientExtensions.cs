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
    /// (exponential backoff with jitter, retrying transient errors and 429/5xx).
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
                ShouldHandle = args => ValueTask.FromResult(ShouldHandle(args.Outcome, aiMode)),
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
                ShouldHandle = args => ValueTask.FromResult(ShouldHandle(args.Outcome, aiMode)),
                OnOpened = args =>
                {
                    onBreak?.Invoke(name, args.Outcome.Result?.StatusCode);
                    return ValueTask.CompletedTask;
                },
            });
        });

        return builder;
    }

    private static bool ShouldHandle(Outcome<HttpResponseMessage> outcome, bool aiMode)
    {
        if (outcome.Exception is not null)
        {
            return true;
        }

        var statusCode = outcome.Result?.StatusCode;
        if (statusCode is null)
        {
            return false;
        }

        if (statusCode == HttpStatusCode.TooManyRequests)
        {
            return !aiMode;
        }

        return (int)statusCode >= 500;
    }
}
