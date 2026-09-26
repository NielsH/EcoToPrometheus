namespace EcoToPrometheus.Hooks
{
    using System;
    using System.Collections.Concurrent;
    using Eco.Gameplay.Players;
    using Eco.Gameplay.Skills;
    using Eco.Gameplay.Systems;
    using EcoToPrometheus.Core;

    /// <summary>
    /// Counts specialty experience as it is granted: <c>eco_player_specialty_experience_gained_total{player,specialty}</c>.
    /// Eco raises <c>Skillset.OnExperienceGained(user, skill)</c> after every grant but without the amount, and a skill's
    /// <c>Experience</c> drops back to the leftover on a level-up, so the hook keeps the last (level, experience) per player
    /// and specialty and reconstructs the delta, adding the thresholds of any levels crossed
    /// (<c>Tier × (level × SpecialtyExperiencePerLevelSquared)²</c>, the same formula as <c>Skill.ExperienceToLevel</c>).
    /// Divided by <c>eco_player_skill_rate</c> this gives XP per rate point, the number that sizes the leveling curve.
    /// Runs on the game thread that granted the XP: one dictionary lookup and an enqueue, body fully isolated.
    /// </summary>
    public sealed class SpecialtyXpHook
    {
        public const string Family = "eco_player_specialty_experience_gained_total";

        readonly MetricsWorker worker;
        readonly ConcurrentDictionary<(int User, Type Skill), (int Level, float Exp)> last = new();
        readonly Action<User, Skill> handler;
        bool attached;

        public SpecialtyXpHook(MetricsWorker worker)
        {
            this.worker  = worker;
            this.handler = this.OnExperienceGained;
        }

        /// <summary>Seeds the tracker from every citizen's current skills, then subscribes. Idempotent.</summary>
        public void Attach()
        {
            if (this.attached) return;
            this.attached = true;
            try
            {
                foreach (var user in UserManager.Users)
                {
                    var skills = user.Skillset?.Skills;
                    if (skills == null) continue;
                    foreach (var skill in skills)
                        if (skill != null) this.last[(user.Id, skill.GetType())] = (skill.Level, skill.Experience);
                }
            }
            catch (Exception ex) { this.worker.RecordError("listener", ex); }
            Skillset.OnExperienceGained.Add(this.handler);
        }

        public void Detach()
        {
            if (!this.attached) return;
            this.attached = false;
            Skillset.OnExperienceGained.Remove(this.handler);
        }

        void OnExperienceGained(User user, Skill skill)
        {
            try
            {
                if (user == null || skill == null) return;
                var key = (user.Id, skill.GetType());
                var now = (skill.Level, skill.Experience);
                if (!this.last.TryGetValue(key, out var prev))
                {
                    this.last[key] = now;   // first sight: nothing to compare against, count from the next grant
                    return;
                }
                this.last[key] = now;

                double delta;
                if (now.Level == prev.Level)
                {
                    delta = now.Experience - prev.Exp;
                }
                else
                {
                    // Crossed one or more levels: the rest of each crossed level's threshold, then the new leftover.
                    // Reaching the max level resets experience to 0, which the same arithmetic handles.
                    var s = BalanceConfig.Obj.SpecialtyExperiencePerLevelSquared;
                    delta = -prev.Exp;
                    for (var level = prev.Level; level < now.Level; level++)
                        delta += skill.Tier * (level * s) * (level * s);
                    delta += now.Experience;
                }
                if (delta <= 0) return;

                var labels   = new[] { new Label(Lbl.Specialty, MetricNames.TypeLabel(skill.GetType().Name, "Skill")) };
                var deferred = new[] { new DeferredLabel(Lbl.Player, RefKind.Player, user.Id) };
                this.worker.Enqueue(new EventRecord(new[] { new CounterIncrement(Family, labels, deferred, delta) }, Environment.TickCount64));
            }
            catch (Exception ex)
            {
                this.worker.RecordError("listener", ex);
            }
        }
    }
}
