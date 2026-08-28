namespace SyntaxCircus.Http.Resilience;

public sealed class HttpRequestTimeoutException : TimeoutException
{
    public HttpRequestTimeoutException(string pipelineName, TimeSpan timeout, Exception? innerException = null)
        : base($"The request timed out after {timeout} in pipeline '{pipelineName}'.", innerException)
    {
        PipelineName = pipelineName;
        Timeout = timeout;
    }

    public string PipelineName { get; }

    public TimeSpan Timeout { get; }
}

public sealed class HttpCircuitOpenException : HttpRequestException
{
    public HttpCircuitOpenException(string pipelineName, TimeSpan? retryAfter, Exception? innerException = null)
        : base($"The circuit is open for pipeline '{pipelineName}'.", innerException)
    {
        PipelineName = pipelineName;
        RetryAfter = retryAfter;
    }

    public string PipelineName { get; }

    public TimeSpan? RetryAfter { get; }
}
