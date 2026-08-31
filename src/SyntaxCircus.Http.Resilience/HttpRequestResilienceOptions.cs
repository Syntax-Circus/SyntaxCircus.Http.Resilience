using System.Collections.Frozen;
using System.Net;

namespace SyntaxCircus.Http.Resilience;

public sealed class HttpRequestResilienceOptions
{
    internal static readonly IReadOnlySet<HttpStatusCode> DefaultRetryableStatusCodes = new HashSet<HttpStatusCode>
    {
        HttpStatusCode.RequestTimeout,
        HttpStatusCode.TooManyRequests,
        HttpStatusCode.InternalServerError,
        HttpStatusCode.BadGateway,
        HttpStatusCode.ServiceUnavailable,
        HttpStatusCode.GatewayTimeout,
    }.ToFrozenSet();

    internal static readonly IReadOnlySet<HttpResilienceFailureCategory> DefaultRetryableExceptionCategories =
        new HashSet<HttpResilienceFailureCategory>
        {
            HttpResilienceFailureCategory.Transport,
            HttpResilienceFailureCategory.Timeout,
        }.ToFrozenSet();

    public int MaxAttempts { get; init; } = 3;

    public TimeSpan TotalRequestTimeout { get; init; } = TimeSpan.FromSeconds(100);

    public TimeSpan BackoffBaseDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    public TimeSpan MaximumDelay { get; init; } = TimeSpan.FromSeconds(30);

    public double CircuitFailureRatio { get; init; } = 0.5;

    public int CircuitMinimumThroughput { get; init; } = 5;

    public TimeSpan CircuitSamplingDuration { get; init; } = TimeSpan.FromSeconds(30);

    public TimeSpan CircuitBreakDuration { get; init; } = TimeSpan.FromSeconds(30);

    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    public Func<double> JitterProvider { get; init; } = Random.Shared.NextDouble;

    public IReadOnlySet<HttpStatusCode> RetryableStatusCodes { get; init; } = DefaultRetryableStatusCodes;

    public IReadOnlySet<HttpResilienceFailureCategory> RetryableExceptionCategories { get; init; } =
        DefaultRetryableExceptionCategories;

    public Func<HttpRetryTelemetry, CancellationToken, ValueTask>? OnRetry { get; init; }

    public Func<HttpTimeoutTelemetry, CancellationToken, ValueTask>? OnTimeout { get; init; }

    public Func<HttpCircuitTelemetry, CancellationToken, ValueTask>? OnCircuitStateChanged { get; init; }
}
