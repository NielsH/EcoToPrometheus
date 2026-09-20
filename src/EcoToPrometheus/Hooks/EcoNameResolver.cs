namespace EcoToPrometheus.Hooks
{
    using System;
    using System.Collections.Concurrent;
    using Eco.Core.Systems;
    using Eco.Gameplay.Civics.Demographics;
    using Eco.Gameplay.Civics.Titles;
    using Eco.Gameplay.Economy;
    using Eco.Gameplay.Players;
    using Eco.Gameplay.Settlements;
    using EcoToPrometheus.Core;

    /// <summary>
    /// Resolves ids captured on the game thread to display names on the worker thread.
    /// Player via <c>UserManager.FindUserByID</c>, the rest by scanning their registrar once and caching. Entries expire after
    /// <see cref="CacheTtl"/> (currencies and settlements can be renamed) and a player entry is dropped on login / first join.
    /// </summary>
    public sealed class EcoNameResolver : INameResolver
    {
        static readonly TimeSpan CacheTtl = TimeSpan.FromMinutes(5);

        readonly ConcurrentDictionary<int, (string Name, long ExpiresTicks)>[] caches;
        readonly Action<User>                                                  invalidatePlayer;
        bool                                                                   subscribed;

        public EcoNameResolver()
        {
            var kinds = Enum.GetValues<RefKind>().Length;
            this.caches = new ConcurrentDictionary<int, (string, long)>[kinds];
            for (int i = 0; i < kinds; i++) this.caches[i] = new ConcurrentDictionary<int, (string, long)>();
            this.invalidatePlayer = user => { try { this.caches[(int)RefKind.Player].TryRemove(user.Id, out _); } catch (Exception) { } };
        }

        /// <summary>Hooks the user events so a renamed or newly created player is re-resolved. Call from Initialize, not the constructor.</summary>
        public void SubscribeInvalidation()
        {
            if (this.subscribed) return;
            this.subscribed = true;
            UserManager.OnUserLoggedIn.Add(this.invalidatePlayer);
            UserManager.NewUserJoinedEvent.Add(this.invalidatePlayer);
        }

        public void UnsubscribeInvalidation()
        {
            if (!this.subscribed) return;
            this.subscribed = false;
            UserManager.OnUserLoggedIn.Remove(this.invalidatePlayer);
            UserManager.NewUserJoinedEvent.Remove(this.invalidatePlayer);
        }

        public string? Resolve(RefKind kind, int id)
        {
            var cache = this.caches[(int)kind];
            var now   = Environment.TickCount64;
            if (cache.TryGetValue(id, out var hit) && hit.ExpiresTicks > now) return hit.Name;

            string? name;
            try { name = Lookup(kind, id); }
            catch (Exception) { name = null; }
            if (string.IsNullOrEmpty(name)) return null; // not cached: the object may appear later (registrars load in parallel)
            cache[id] = (name, now + (long)CacheTtl.TotalMilliseconds);
            return name;
        }

        static string? Lookup(RefKind kind, int id) => kind switch
        {
            RefKind.Player       => UserManager.FindUserByID(id)?.Name,
            RefKind.Currency     => FindById(Registrars.Get<Currency>(), id)?.Name,
            RefKind.Settlement   => FindById(Registrars.Get<Settlement>(), id)?.Name,
            RefKind.Demographic  => FindById(Registrars.Get<Demographic>(), id)?.Name,
            RefKind.ElectedTitle => FindById(Registrars.Get<ElectedTitle>(), id)?.Name,
            _                    => null,
        };

        static T? FindById<T>(Registrar<T> registrar, int id) where T : class, IHasID
        {
            foreach (var entry in registrar)
                if (entry.Id == id) return entry;
            return null;
        }
    }
}
