namespace SyntaxCircus.Http.Resilience;

/// <summary>An API call failed with an RFC 7807 ProblemDetails response (or a non-success status with no body to parse).</summary>
public sealed class ProblemDetailsException : Exception
{
    public int? StatusCode { get; }

    public string? Type { get; }

    public string? Title { get; }

    public IReadOnlyDictionary<string, string[]>? Errors { get; }

    public ProblemDetailsException(int? statusCode, string? type, string? title, string? detail, IReadOnlyDictionary<string, string[]>? errors)
        : base(detail ?? title ?? "The request failed.")
    {
        StatusCode = statusCode;
        Type = type;
        Title = title;
        Errors = errors;
    }
}
