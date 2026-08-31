using System.Net;

namespace SyntaxCircus.Http.Resilience.Tests;

public class HttpRequestResilienceOptionsTests
{
    [Fact]
    public void Options_HaveDocumentedDefaults()
    {
        var options = new HttpRequestResilienceOptions();

        options.MaxAttempts.ShouldBe(3);
        options.TotalRequestTimeout.ShouldBe(TimeSpan.FromSeconds(100));
        options.BackoffBaseDelay.ShouldBe(TimeSpan.FromMilliseconds(100));
        options.MaximumDelay.ShouldBe(TimeSpan.FromSeconds(30));
        options.CircuitFailureRatio.ShouldBe(0.5);
        options.CircuitMinimumThroughput.ShouldBe(5);
        options.CircuitSamplingDuration.ShouldBe(TimeSpan.FromSeconds(30));
        options.CircuitBreakDuration.ShouldBe(TimeSpan.FromSeconds(30));
        options.TimeProvider.ShouldBeSameAs(TimeProvider.System);
        options.JitterProvider.ShouldNotBeNull();
        options.RetryableStatusCodes.ShouldBe([
            HttpStatusCode.RequestTimeout,
            HttpStatusCode.TooManyRequests,
            HttpStatusCode.InternalServerError,
            HttpStatusCode.BadGateway,
            HttpStatusCode.ServiceUnavailable,
            HttpStatusCode.GatewayTimeout,
        ], ignoreOrder: true);
        options.RetryableExceptionCategories.ShouldBe([
            HttpResilienceFailureCategory.Transport,
            HttpResilienceFailureCategory.Timeout,
        ], ignoreOrder: true);
        options.OnRetry.ShouldBeNull();
        options.OnTimeout.ShouldBeNull();
        options.OnCircuitStateChanged.ShouldBeNull();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Constructor_RejectsBlankName(string? name)
    {
        Should.Throw<ArgumentException>(() => new HttpRequestResiliencePipeline(name!, new HttpRequestResilienceOptions()));
    }

    [Fact]
    public void Constructor_RejectsNullOptions()
    {
        Should.Throw<ArgumentNullException>(() => new HttpRequestResiliencePipeline("cmsify", null!));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_RejectsNonPositiveMaxAttempts(int value)
    {
        var options = new HttpRequestResilienceOptions { MaxAttempts = value };

        Should.Throw<ArgumentOutOfRangeException>(() => new HttpRequestResiliencePipeline("cmsify", options));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_RejectsNonPositiveTotalRequestTimeout(int milliseconds)
    {
        var options = new HttpRequestResilienceOptions { TotalRequestTimeout = TimeSpan.FromMilliseconds(milliseconds) };

        Should.Throw<ArgumentOutOfRangeException>(() => new HttpRequestResiliencePipeline("cmsify", options));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_RejectsNonPositiveBackoffBaseDelay(int milliseconds)
    {
        var options = new HttpRequestResilienceOptions { BackoffBaseDelay = TimeSpan.FromMilliseconds(milliseconds) };

        Should.Throw<ArgumentOutOfRangeException>(() => new HttpRequestResiliencePipeline("cmsify", options));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_RejectsNonPositiveMaximumDelay(int milliseconds)
    {
        var options = new HttpRequestResilienceOptions { MaximumDelay = TimeSpan.FromMilliseconds(milliseconds) };

        Should.Throw<ArgumentOutOfRangeException>(() => new HttpRequestResiliencePipeline("cmsify", options));
    }

    [Fact]
    public void Constructor_RejectsBackoffBaseDelayGreaterThanMaximumDelay()
    {
        var options = new HttpRequestResilienceOptions
        {
            BackoffBaseDelay = TimeSpan.FromSeconds(2),
            MaximumDelay = TimeSpan.FromSeconds(1),
        };

        Should.Throw<ArgumentOutOfRangeException>(() => new HttpRequestResiliencePipeline("cmsify", options));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-0.1)]
    [InlineData(1.1)]
    public void Constructor_RejectsCircuitFailureRatioOutsidePermittedRange(double value)
    {
        var options = new HttpRequestResilienceOptions { CircuitFailureRatio = value };

        Should.Throw<ArgumentOutOfRangeException>(() => new HttpRequestResiliencePipeline("cmsify", options));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    public void Constructor_RejectsCircuitMinimumThroughputBelowTwo(int value)
    {
        var options = new HttpRequestResilienceOptions { CircuitMinimumThroughput = value };

        Should.Throw<ArgumentOutOfRangeException>(() => new HttpRequestResiliencePipeline("cmsify", options));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_RejectsNonPositiveCircuitSamplingDuration(int milliseconds)
    {
        var options = new HttpRequestResilienceOptions { CircuitSamplingDuration = TimeSpan.FromMilliseconds(milliseconds) };

        Should.Throw<ArgumentOutOfRangeException>(() => new HttpRequestResiliencePipeline("cmsify", options));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_RejectsNonPositiveCircuitBreakDuration(int milliseconds)
    {
        var options = new HttpRequestResilienceOptions { CircuitBreakDuration = TimeSpan.FromMilliseconds(milliseconds) };

        Should.Throw<ArgumentOutOfRangeException>(() => new HttpRequestResiliencePipeline("cmsify", options));
    }

    [Fact]
    public void Constructor_RejectsNullTimeProvider()
    {
        var options = new HttpRequestResilienceOptions { TimeProvider = null! };

        Should.Throw<ArgumentNullException>(() => new HttpRequestResiliencePipeline("cmsify", options));
    }

    [Fact]
    public void Constructor_RejectsNullJitterProvider()
    {
        var options = new HttpRequestResilienceOptions { JitterProvider = null! };

        Should.Throw<ArgumentNullException>(() => new HttpRequestResiliencePipeline("cmsify", options));
    }

    [Fact]
    public void Constructor_RejectsNullRetryableStatusCodes()
    {
        var options = new HttpRequestResilienceOptions { RetryableStatusCodes = null! };

        Should.Throw<ArgumentNullException>(() => new HttpRequestResiliencePipeline("cmsify", options));
    }

    [Fact]
    public void Constructor_RejectsNullRetryableExceptionCategories()
    {
        var options = new HttpRequestResilienceOptions { RetryableExceptionCategories = null! };

        Should.Throw<ArgumentNullException>(() => new HttpRequestResiliencePipeline("cmsify", options));
    }

    [Theory]
    [InlineData(HttpResilienceFailureCategory.HttpStatus)]
    [InlineData(HttpResilienceFailureCategory.CircuitOpen)]
    [InlineData((HttpResilienceFailureCategory)999)]
    public void Constructor_RejectsUnsupportedRetryableExceptionCategory(HttpResilienceFailureCategory category)
    {
        var options = new HttpRequestResilienceOptions
        {
            RetryableExceptionCategories = new HashSet<HttpResilienceFailureCategory> { category },
        };

        Should.Throw<ArgumentOutOfRangeException>(() => new HttpRequestResiliencePipeline("cmsify", options));
    }

    [Fact]
    public void Constructor_CopiesOptionValues()
    {
        var options = new HttpRequestResilienceOptions
        {
            MaxAttempts = 7,
            TotalRequestTimeout = TimeSpan.FromSeconds(11),
            BackoffBaseDelay = TimeSpan.FromMilliseconds(250),
            MaximumDelay = TimeSpan.FromSeconds(9),
            CircuitFailureRatio = 0.7,
            CircuitMinimumThroughput = 9,
            CircuitSamplingDuration = TimeSpan.FromSeconds(12),
            CircuitBreakDuration = TimeSpan.FromSeconds(13),
        };

        var pipeline = new HttpRequestResiliencePipeline("cmsify", options);
        var optionsField = typeof(HttpRequestResiliencePipeline).GetField("_options", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);

        optionsField.ShouldNotBeNull();
        var copiedOptions = optionsField.GetValue(pipeline).ShouldBeOfType<HttpRequestResilienceOptions>();
        copiedOptions.ShouldNotBeSameAs(options);
        copiedOptions.MaxAttempts.ShouldBe(options.MaxAttempts);
        copiedOptions.TotalRequestTimeout.ShouldBe(options.TotalRequestTimeout);
        copiedOptions.BackoffBaseDelay.ShouldBe(options.BackoffBaseDelay);
        copiedOptions.MaximumDelay.ShouldBe(options.MaximumDelay);
        copiedOptions.CircuitFailureRatio.ShouldBe(options.CircuitFailureRatio);
        copiedOptions.CircuitMinimumThroughput.ShouldBe(options.CircuitMinimumThroughput);
        copiedOptions.CircuitSamplingDuration.ShouldBe(options.CircuitSamplingDuration);
        copiedOptions.CircuitBreakDuration.ShouldBe(options.CircuitBreakDuration);
    }

    [Fact]
    public void Constructor_RetainsValidatedPipelineName()
    {
        var pipeline = new HttpRequestResiliencePipeline("cmsify", new HttpRequestResilienceOptions());
        var nameField = typeof(HttpRequestResiliencePipeline).GetField("_name", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);

        nameField.ShouldNotBeNull();
        nameField.GetValue(pipeline).ShouldBe("cmsify");
    }

    [Fact]
    public void TimeoutException_ExposesPipelineNameTimeoutAndInnerException()
    {
        var innerException = new InvalidOperationException("inner");

        var exception = new HttpRequestTimeoutException("cmsify", TimeSpan.FromSeconds(7), innerException);

        exception.PipelineName.ShouldBe("cmsify");
        exception.Timeout.ShouldBe(TimeSpan.FromSeconds(7));
        exception.InnerException.ShouldBeSameAs(innerException);
    }

    [Fact]
    public void CircuitOpenException_ExposesPipelineNameRetryAfterAndInnerException()
    {
        var innerException = new InvalidOperationException("inner");

        var exception = new HttpCircuitOpenException("cmsify", TimeSpan.FromSeconds(7), innerException);

        exception.PipelineName.ShouldBe("cmsify");
        exception.RetryAfter.ShouldBe(TimeSpan.FromSeconds(7));
        exception.InnerException.ShouldBeSameAs(innerException);
    }
}
