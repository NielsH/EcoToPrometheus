namespace EcoToPrometheus.Hooks
{
    using System;
    using Eco.Gameplay.Items;
    using Eco.Gameplay.Objects;
    using Eco.Gameplay.Players;
    using EcoToPrometheus.Core;

    /// <summary>
    /// Food is not a GameAction; it comes from <c>Stomach.GlobalFoodEatenEvent</c>. Enqueues
    /// <c>eco_food_eaten_total{food}</c> (+player when <c>FoodEaten</c> is allowlisted) and
    /// <c>eco_food_calories_eaten_total{food}</c> with the item's calories. Same isolation as the action listener.
    /// </summary>
    public sealed class FoodEatenHook
    {
        readonly MetricsWorker                         worker;
        readonly Action<User, FoodItem, WorldObject>   handler;
        volatile bool                                  withPlayer;
        volatile bool                                  excluded;
        bool                                           attached;

        public FoodEatenHook(MetricsWorker worker)
        {
            this.worker  = worker;
            this.handler = this.OnFoodEaten;
        }

        public bool Attached => this.attached;

        /// <summary>Re-reads the allow/exclude state from the current rule set (called whenever the rules are rebuilt).</summary>
        public void Configure(FamilyRuleSet rules)
        {
            this.withPlayer = rules.WithPlayer(FamilyRules.FoodEatenSource);
            this.excluded   = rules.IsExcluded(FamilyRules.FoodEatenSource);
        }

        public void Attach()
        {
            if (this.attached) return;
            this.attached = true;
            Stomach.GlobalFoodEatenEvent.Add(this.handler);
        }

        public void Detach()
        {
            if (!this.attached) return;
            this.attached = false;
            Stomach.GlobalFoodEatenEvent.Remove(this.handler);
        }

        void OnFoodEaten(User user, FoodItem food, WorldObject table)
        {
            try
            {
                if (this.excluded || food == null) return;
                var labels   = new[] { new Label(Lbl.Food, TypeLabels.Item(food)) };
                var deferred = this.withPlayer && user != null
                    ? new[] { new DeferredLabel(Lbl.Player, RefKind.Player, user.Id) }
                    : CounterIncrement.NoDeferred;
                var increments = new[]
                {
                    new CounterIncrement(FamilyRules.FoodEaten,    labels, deferred, 1),
                    new CounterIncrement(FamilyRules.FoodCalories, labels, CounterIncrement.NoDeferred, food.Calories),
                };
                this.worker.Enqueue(new EventRecord(increments, Environment.TickCount64));
            }
            catch (Exception ex)
            {
                this.worker.RecordError("listener", ex);
            }
        }
    }
}
