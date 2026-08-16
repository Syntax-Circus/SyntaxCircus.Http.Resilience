namespace SyntaxCircus.Http.Resilience.Tests;

public class ProblemDetailsExceptionTests
{
    [Fact]
    public void Constructor_SetsAllProperties()
    {
        var errors = new Dictionary<string, string[]> { ["field"] = ["required"] };

        var exception = new ProblemDetailsException(409, "https://example.com/conflict", "Conflict", "already exists", errors);

        exception.StatusCode.ShouldBe(409);
        exception.Type.ShouldBe("https://example.com/conflict");
        exception.Title.ShouldBe("Conflict");
        exception.Errors.ShouldBe(errors);
    }

    [Fact]
    public void Constructor_DetailProvided_MessageIsDetail()
    {
        var exception = new ProblemDetailsException(400, null, "Bad Request", "specific detail", null);

        exception.Message.ShouldBe("specific detail");
    }

    [Fact]
    public void Constructor_NoDetailButTitleProvided_MessageIsTitle()
    {
        var exception = new ProblemDetailsException(400, null, "Bad Request", null, null);

        exception.Message.ShouldBe("Bad Request");
    }

    [Fact]
    public void Constructor_NoDetailNoTitle_MessageIsGenericFallback()
    {
        var exception = new ProblemDetailsException(500, null, null, null, null);

        exception.Message.ShouldBe("The request failed.");
    }
}
