using System.Net;

namespace SyntaxCircus.Http.Resilience;

public enum HttpRequestReplaySafety
{
    NotReplayable,
    Replayable,
}

public enum HttpResilienceFailureCategory
{
    HttpStatus,
    Transport,
    Timeout,
    CircuitOpen,
}

public enum HttpResilienceCircuitState
{
    Open,
    HalfOpen,
    Closed,
}

public sealed record HttpRetryTelemetry(
    string PipelineName,
    int AttemptNumber,
    HttpStatusCode? StatusCode,
    HttpResilienceFailureCategory FailureCategory,
    TimeSpan Delay);

public sealed record HttpCircuitTelemetry(
    string PipelineName,
    HttpResilienceCircuitState State,
    HttpStatusCode? StatusCode,
    HttpResilienceFailureCategory FailureCategory);
