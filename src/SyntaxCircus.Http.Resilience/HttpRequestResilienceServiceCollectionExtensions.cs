namespace SyntaxCircus.Http.Resilience;

public static class HttpRequestResilienceServiceCollectionExtensions
{
    public static IServiceCollection AddHttpRequestResiliencePipeline(
        this IServiceCollection services,
        string name,
        HttpRequestResilienceOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(options);

        services.AddKeyedSingleton<HttpRequestResiliencePipeline>(
            name,
            (_, _) => new HttpRequestResiliencePipeline(name, options));

        return services;
    }
}
