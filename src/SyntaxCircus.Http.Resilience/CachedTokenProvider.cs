namespace SyntaxCircus.Http.Resilience;

public sealed record CachedToken(string Value, DateTimeOffset ExpiresAt);

/// <summary>
/// A generic, semaphore-guarded cache for a single bearer/access token, refreshed on demand via a
/// caller-supplied acquisition delegate. Useful for worker-to-API client-credentials auth, or any
/// other "fetch a token, cache it until near expiry, refresh under a lock" scenario — independent
/// of any specific token source (OAuth2 client-credentials, a static API's own token endpoint, etc).
/// </summary>
public sealed class CachedTokenProvider(
    Func<CancellationToken, Task<CachedToken>> acquireToken,
    TimeSpan? expirySkew = null) : IDisposable
{
    private readonly TimeSpan _expirySkew = expirySkew ?? TimeSpan.FromSeconds(60);
    private readonly SemaphoreSlim _refreshLock = new(1, 1);
    private CachedToken? _current;

    public async Task<string> GetTokenAsync(CancellationToken cancellationToken = default)
    {
        var current = _current;
        if (current is not null && current.ExpiresAt - _expirySkew > DateTimeOffset.UtcNow)
        {
            return current.Value;
        }

        await _refreshLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            current = _current;
            if (current is not null && current.ExpiresAt - _expirySkew > DateTimeOffset.UtcNow)
            {
                return current.Value;
            }

            var acquired = await acquireToken(cancellationToken).ConfigureAwait(false);
            _current = acquired;
            return acquired.Value;
        }
        finally
        {
            _refreshLock.Release();
        }
    }

    public void Dispose() => _refreshLock.Dispose();
}
