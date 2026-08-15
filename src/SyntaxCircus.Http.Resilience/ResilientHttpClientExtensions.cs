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
    public static IHttpClientBuilder AddResilientHttpClient(
        this IServiceCollection services,
        string name,
        Action<HttpClient>? configureClient = null,
        int retryCount = 2,
        bool aiMode = false)
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
            });

            pipelineBuilder.AddCircuitBreaker(new CircuitBreakerStrategyOptions<HttpResponseMessage>
            {
                FailureRatio = 0.5,
                MinimumThroughput = 5,
                BreakDuration = TimeSpan.FromSeconds(30),
                ShouldHandle = args => ValueTask.FromResult(ShouldHandle(args.Outcome, aiMode)),
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
