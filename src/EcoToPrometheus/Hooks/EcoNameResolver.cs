namespace EcoToPrometheus.Hooks
{
    using EcoToPrometheus.Core;

    /// <summary>
    /// Resolves ids captured on the game thread to display names on the worker thread.
    /// Phase 3: Player via UserManager.FindUserByID(id)?.Name (cached, refreshed on login/new-user events),
    /// Currency/Settlement/Demographic/ElectedTitle via Registrars.Get&lt;T&gt;() lookups, cached per worker run.
    /// Phase 1 returns null so the worker emits placeholders such as <c>player_42</c>.
    /// </summary>
    public sealed class EcoNameResolver : INameResolver
    {
        public string? Resolve(RefKind kind, int id) => null;
    }
}
