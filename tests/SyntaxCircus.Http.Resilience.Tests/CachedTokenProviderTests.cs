namespace SyntaxCircus.Http.Resilience.Tests;

public class CachedTokenProviderTests
{
    [Fact]
    public async Task GetTokenAsync_FirstCall_InvokesAcquireToken()
    {
        var callCount = 0;
        using var provider = new CachedTokenProvider(ct =>
        {
            callCount++;
            return Task.FromResult(new CachedToken("token-1", DateTimeOffset.UtcNow.AddMinutes(10)));
        });

        var token = await provider.GetTokenAsync(TestContext.Current.CancellationToken);

        token.ShouldBe("token-1");
        callCount.ShouldBe(1);
    }

    [Fact]
    public async Task GetTokenAsync_TokenStillValid_DoesNotReacquire()
    {
        var callCount = 0;
        using var provider = new CachedTokenProvider(ct =>
        {
            callCount++;
            return Task.FromResult(new CachedToken("token-1", DateTimeOffset.UtcNow.AddMinutes(10)));
        });

        await provider.GetTokenAsync(TestContext.Current.CancellationToken);
        await provider.GetTokenAsync(TestContext.Current.CancellationToken);

        callCount.ShouldBe(1);
    }

    [Fact]
    public async Task GetTokenAsync_TokenWithinExpirySkew_Reacquires()
    {
        var callCount = 0;
        using var provider = new CachedTokenProvider(
            ct =>
            {
                callCount++;
                return Task.FromResult(new CachedToken($"token-{callCount}", DateTimeOffset.UtcNow.AddMinutes(1)));
            },
            expirySkew: TimeSpan.FromMinutes(5));

        var first = await provider.GetTokenAsync(TestContext.Current.CancellationToken);
        var second = await provider.GetTokenAsync(TestContext.Current.CancellationToken);

        callCount.ShouldBe(2);
        first.ShouldBe("token-1");
        second.ShouldBe("token-2");
    }

    [Fact]
    public async Task GetTokenAsync_ConcurrentCallsWhileRefreshing_OnlyAcquiresOnce()
    {
        var callCount = 0;
        using var provider = new CachedTokenProvider(async ct =>
        {
            Interlocked.Increment(ref callCount);
            await Task.Delay(50, ct).ConfigureAwait(false);
            return new CachedToken("token-1", DateTimeOffset.UtcNow.AddMinutes(10));
        });

        var results = await Task.WhenAll(
            provider.GetTokenAsync(TestContext.Current.CancellationToken),
            provider.GetTokenAsync(TestContext.Current.CancellationToken),
            provider.GetTokenAsync(TestContext.Current.CancellationToken));

        callCount.ShouldBe(1);
        results.ShouldAllBe(token => token == "token-1");
    }

    [Fact]
    public void Dispose_DoesNotThrow()
    {
        var provider = new CachedTokenProvider(_ => Task.FromResult(new CachedToken("t", DateTimeOffset.UtcNow.AddMinutes(10))));

        Should.NotThrow(provider.Dispose);
    }
}
