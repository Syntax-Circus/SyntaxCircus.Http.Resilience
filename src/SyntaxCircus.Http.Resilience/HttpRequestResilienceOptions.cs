namespace SyntaxCircus.Http.Resilience;

public sealed class HttpRequestResilienceOptions
{
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

    public Func<HttpRetryTelemetry, CancellationToken, ValueTask>? OnRetry { get; init; }

    public Func<HttpCircuitTelemetry, CancellationToken, ValueTask>? OnCircuitStateChanged { get; init; }
}
