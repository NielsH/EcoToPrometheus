// Rig-only test helper for EcoToPrometheus. Copy to Build\Server\win-x64\Mods\UserCode\MetricsRig\ on the local rig;
// it is NOT part of the mod DLL. Compiled by the server's Roslyn together with every other Mods source, so it can only
// reference the core Eco assemblies: the metrics plugin is reached by reflection.
namespace Eco.Mods.MetricsRig
{
    using System;
    using System.Collections;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using Eco.Gameplay.Economy;
    using Eco.Gameplay.GameActions;
    using Eco.Gameplay.Items;
    using Eco.Gameplay.Players;
    using Eco.Gameplay.Systems.Chat;
    using Eco.Gameplay.Systems.Messaging.Chat.Commands;
    using Eco.Mods.Organisms;
    using Eco.Mods.TechTree;
    using Eco.Shared.Items;
    using Eco.Shared.Localization;
    using Eco.Shared.Logging;

    /// <summary>Fires synthetic GameActions straight at the global listeners (ActionUtil.ActionPerformed), bypassing
    /// GameActionManager validation, so the metrics listener is exercised with real action objects without a client.
    /// Headless calls (POST /api/v1/command/exec) carry no user, so the citizen is the first known user.</summary>
    [ChatCommandHandler]
    public static class MetricsRigCommands
    {
        [ChatCommand("Rig-only: fires synthetic GameActions at the metrics listener. what = chop|mine|craft|trade|play|nostats|all|dump; count = repetitions.")]
        public static void MetricsRig(IChatClient chatClient, string what = "all", int count = 5)
        {
            var user = chatClient as User ?? UserManager.Users.FirstOrDefault();
            what = (what ?? "all").Trim().ToLowerInvariant();
            if (what == "dump") { Dump(chatClient); return; }

            var fired = 0;
            void Fire(Func<GameAction> make) { for (var i = 0; i < count; i++) { ActionUtil.ActionPerformed(make()); fired++; } }

            var all = what == "all";
            if (all || what == "chop")    Fire(() => new ChopTree          { Citizen = user, Species = typeof(Oak.OakSpecies), ToolUsed = Item.Get<IronAxeItem>(), Felled = true, CaloriesToConsume = 20f, GrowthPercent = 100f });
            if (all || what == "mine")    Fire(() => new DigOrMine         { Citizen = user, ItemUsed = Item.Get<SandstoneItem>(), ToolUsed = Item.Get<IronPickaxeItem>(), CaloriesToConsume = 15f });
            if (all || what == "craft")   Fire(() => new ItemCraftedAction { Citizen = user, ItemUsed = Item.Get<HewnLogItem>() });
            if (all || what == "trade")   Fire(() => new CurrencyTrade     { Citizen = user, ItemUsed = Item.Get<WheatItem>(), NumberOfItems = 10f, BoughtOrSold = BoughtOrSold.Buying, Currency = CurrencyManager.Currencies.FirstOrDefault(), CurrencyAmount = 25.5f, Buyer = user, Seller = user, ShopOwner = user });
            if (all || what == "play")    Fire(() => new Play              { Citizen = user, SecondsPassed = 30f });
            if (what == "xp")             // grants SkillRate x 50 specialty XP to Self Improvement per repetition (rig S=25: level 1->2 at 625)
            {
                for (var i = 0; i < count; i++) user.Skillset.AddExperience(typeof(SelfImprovementSkill), 50f, Localizer.DoStr("metricsrig"));
                chatClient.MsgLoc($"[metricsrig] granted {count} x (skill rate {user.UserXP.SkillRate:F0} x 50) XP to {user.Name}'s Self Improvement (now level {user.Skillset.GetSkill(typeof(SelfImprovementSkill))?.Level}, exp {user.Skillset.GetSkill(typeof(SelfImprovementSkill))?.Experience:F0})");
                return;
            }
            if (all || what == "nostats")
            {
                Fire(() => new ChopTree   { Citizen = user, Species = typeof(Oak.OakSpecies), ToolUsed = Item.Get<IronAxeItem>(), Felled = false }); // IConditionalStatistics declines
                Fire(() => new OpenAction { Citizen = user });                                                                                    // [NoStats]
            }

            var msg = $"[MetricsRig] {what}: fired {fired} action(s) as {(user?.Name ?? "<no user>")}.";
            Log.WriteLine(Localizer.DoStr(msg));
            chatClient.MsgLoc($"{msg}");
        }

        /// <summary>Walks MetricsPlugin.Obj.Registry.Current by reflection and logs every series as Prometheus-style text.</summary>
        static void Dump(IChatClient chatClient)
        {
            var asm = AppDomain.CurrentDomain.GetAssemblies().FirstOrDefault(a => a.GetName().Name == "EcoToPrometheus");
            var pluginType = asm?.GetType("EcoToPrometheus.MetricsPlugin");
            var plugin     = pluginType?.GetProperty("Obj", BindingFlags.Public | BindingFlags.Static)?.GetValue(null);
            if (plugin == null) { chatClient.MsgLoc($"[MetricsRig] MetricsPlugin not loaded."); return; }

            var registry = Prop(plugin, "Registry");
            var current  = Prop(registry, "Current");
            var families = (IEnumerable)Prop(current, "Families");
            var lines    = 0;
            var sb       = new StringBuilder();
            foreach (var family in families)
            {
                var name = (string)Prop(family, "Name");
                var type = Prop(family, "Type").ToString().ToLowerInvariant();
                var help = (string)Prop(family, "Help");
                Log.WriteLine(Localizer.DoStr($"[MetricsRig] # HELP {name} {help}"));
                Log.WriteLine(Localizer.DoStr($"[MetricsRig] # TYPE {name} {type}"));
                foreach (var series in (IEnumerable)Prop(family, "Series"))
                {
                    sb.Clear().Append(name);
                    var labels = (IEnumerable)Prop(series, "Labels");
                    var first  = true;
                    foreach (var label in labels)
                    {
                        sb.Append(first ? '{' : ',').Append((string)Prop(label, "Name")).Append("=\"").Append((string)Prop(label, "Value")).Append('"');
                        first = false;
                    }
                    if (!first) sb.Append('}');
                    sb.Append(' ').Append(Convert.ToDouble(Prop(series, "Value")).ToString(System.Globalization.CultureInfo.InvariantCulture));
                    Log.WriteLine(Localizer.DoStr("[MetricsRig] " + sb));
                    lines++;
                }
            }
            var summary = $"[MetricsRig] dump: {lines} series written to the server log.";
            Log.WriteLine(Localizer.DoStr(summary));
            chatClient.MsgLoc($"{summary}");
        }

        static object Prop(object obj, string name) => obj.GetType().GetProperty(name, BindingFlags.Public | BindingFlags.Instance).GetValue(obj);
    }
}
