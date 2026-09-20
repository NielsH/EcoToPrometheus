namespace EcoToPrometheus.Hooks
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Eco.Core.Systems;
    using Eco.Gameplay.Civics.Demographics;
    using Eco.Gameplay.Civics.Titles;
    using Eco.Gameplay.Economy;
    using Eco.Gameplay.GameActions;
    using Eco.Gameplay.Items;
    using Eco.Gameplay.Players;
    using Eco.Gameplay.Settlements;
    using Eco.Gameplay.Settlements.Civics;
    using Eco.Shared.Items;
    using Eco.Stats;
    using EcoToPrometheus.Core;

    /// <summary>
    /// How one GameAction type turns into counter increments. <see cref="Build"/> runs on the game thread:
    /// it may read fields, type names and enums, and capture ids; it must not touch registrars or format strings.
    /// </summary>
    public sealed class FamilyRule
    {
        public string SourceName { get; }                          // GameAction type name, e.g. "ChopTree"
        public string Family     { get; }                          // eco_action_chop_tree_total
        public bool   Curated    { get; }                          // false for the automatic rule given to unknown types
        public Func<GameAction, bool, CounterIncrement[]> Build { get; } // (action, withPlayerLabel) -> increments

        public FamilyRule(string sourceName, string family, Func<GameAction, bool, CounterIncrement[]> build, bool curated = true)
        {
            this.SourceName = sourceName;
            this.Family     = family;
            this.Build      = build;
            this.Curated    = curated;
        }
    }

    /// <summary>HELP/TYPE for one family, collected at build time and registered once from the worker thread.</summary>
    public readonly record struct FamilyHelp(string Family, MetricType Type, string Help);

    /// <summary>
    /// Immutable Type -> rule map plus the config-derived allow/exclude sets. The listener holds a volatile reference,
    /// the worker builds a new one when config changes. The empty set enqueues nothing.
    /// </summary>
    public sealed class FamilyRuleSet
    {
        public static readonly FamilyRuleSet Empty = new(new Dictionary<Type, FamilyRule>(), new HashSet<string>(StringComparer.OrdinalIgnoreCase), new HashSet<string>(StringComparer.OrdinalIgnoreCase));

        readonly Dictionary<Type, FamilyRule> byType;
        readonly HashSet<Type>                noStatsTypes;
        readonly HashSet<string>              playerLabelFamilies;
        readonly HashSet<string>              excludedFamilies;
        readonly List<FamilyHelp>             help;

        public FamilyRuleSet(Dictionary<Type, FamilyRule> byType, HashSet<string> playerLabelFamilies, HashSet<string> excludedFamilies)
            : this(byType, new HashSet<Type>(), playerLabelFamilies, excludedFamilies, new List<FamilyHelp>(), Array.Empty<string>()) { }

        public FamilyRuleSet(Dictionary<Type, FamilyRule> byType, HashSet<Type> noStatsTypes, HashSet<string> playerLabelFamilies, HashSet<string> excludedFamilies, List<FamilyHelp> help, IReadOnlyList<string> warnings)
        {
            this.byType              = byType;
            this.noStatsTypes        = noStatsTypes;
            this.playerLabelFamilies = playerLabelFamilies;
            this.excludedFamilies    = excludedFamilies;
            this.help                = help;
            this.Warnings            = warnings;
            foreach (var rule in byType.Values) { if (rule.Curated) this.CuratedCount++; else this.AutoCount++; }
        }

        public int                   Count        => this.byType.Count;
        public int                   CuratedCount { get; }
        public int                   AutoCount    { get; }
        public int                   NoStatsCount => this.noStatsTypes.Count;
        public IReadOnlyList<string> Warnings     { get; }

        /// <summary>True for types Eco itself never records (<c>[NoStats]</c>); cached per type at build time.</summary>
        public bool IsNoStats(Type type) => this.noStatsTypes.Contains(type);

        /// <summary>True when the pseudo source <c>FoodEaten</c> (or any source name) should carry a player label.</summary>
        public bool WithPlayer(string sourceName) => this.playerLabelFamilies.Contains(sourceName);
        public bool IsExcluded(string sourceName) => this.excludedFamilies.Contains(sourceName);

        /// <summary>Returns null when the action is excluded or unknown to this set. The Eco-stats filter runs in the listener before this.</summary>
        public EventRecord? Build(GameAction action)
        {
            if (this.byType.Count == 0) return null;
            if (!this.byType.TryGetValue(action.GetType(), out var rule)) return null;
            if (this.excludedFamilies.Contains(rule.SourceName)) return null;
            var increments = rule.Build(action, this.playerLabelFamilies.Contains(rule.SourceName));
            return increments.Length == 0 ? null : new EventRecord(increments, Environment.TickCount64);
        }

        /// <summary>Registers HELP/TYPE for every count and value family. Worker thread only (the registry is not thread-safe).</summary>
        public void RegisterHelp(MetricRegistry registry)
        {
            foreach (var h in this.help)              registry.RegisterFamily(h.Family, h.Type, h.Help);
            foreach (var h in FamilyRules.ValueHelp)  registry.RegisterFamily(h.Family, h.Type, h.Help);
        }
    }

    /// <summary>Label names used by the catalogue (the rename table of the spec).</summary>
    public static class Lbl
    {
        public const string Player = "player", Currency = "currency", Settlement = "settlement", Action = "action";
        public const string Op = "op", Side = "side", Kind = "kind", Block = "block", Item = "item", Tool = "tool", Table = "table", Object = "object";
        public const string Species = "species", Reason = "reason", Change = "change", Sign = "sign", Source = "source", SettlementType = "settlement_type";
        public const string VoteType = "vote_type", Method = "method", Specialty = "specialty", Profession = "profession", Demographic = "demographic";
        public const string Title = "title", Stage = "stage", Part = "part", FirstTime = "first_time", Food = "food", Type = "type";
    }

    /// <summary>Per-Type label strings, computed once so the per-event path does no substring work.</summary>
    static class TypeLabels
    {
        static readonly ConcurrentDictionary<Type, string> items   = new();
        static readonly ConcurrentDictionary<Type, string> species = new();
        static readonly ConcurrentDictionary<Type, string> skills  = new();

        public static string Item(Item? item)     => item == null ? string.Empty : items.GetOrAdd(item.GetType(), static t => MetricNames.TypeLabel(t.Name, "Item"));
        public static string Species(Type? type)  => type == null ? string.Empty : species.GetOrAdd(type, static t => MetricNames.TypeLabel(t.Name, "Species"));
        public static string Skill(Item? skill)   => skill == null ? string.Empty : skills.GetOrAdd(skill.GetType(), static t => MetricNames.TypeLabel(t.Name, "Skill"));
    }

    /// <summary>Enum member names without the per-call reflection of <c>Enum.ToString()</c>.</summary>
    static class EnumLabel<TEnum> where TEnum : struct, Enum
    {
        static readonly Dictionary<TEnum, string> names = Enum.GetValues<TEnum>().Distinct().ToDictionary(v => v, v => v.ToString());
        public static string Of(TEnum value) => names.TryGetValue(value, out var s) ? s : value.ToString();
    }

    /// <summary>The catalogue in code. Builds a <see cref="FamilyRuleSet"/> from the loaded GameAction types and the config.</summary>
    public static class FamilyRules
    {
        public const string FoodEatenSource = "FoodEaten";

        static readonly Label[]         NoLabels = CounterIncrement.NoLabels;
        static readonly DeferredLabel[] NoDef    = CounterIncrement.NoDeferred;

        // ---- value families (companion counters) ------------------------------------------------

        public const string Playtime          = "eco_playtime_seconds_total";
        public const string Calories          = "eco_calories_consumed_total";
        public const string TradeItems        = "eco_trade_items_total";
        public const string TradeCurrency     = "eco_trade_currency_total";
        public const string MoneyTransferred  = "eco_money_transferred_total";
        public const string TaxPaid           = "eco_tax_paid_total";
        public const string TaxBase           = "eco_tax_base_total";
        public const string GovFunds          = "eco_government_funds_received_total";
        public const string RentPaid          = "eco_rent_paid_total";
        public const string WagesPaid         = "eco_wages_paid_total";
        public const string CurrencyMinted    = "eco_currency_minted_total";
        public const string CulturalSales     = "eco_cultural_object_sales_total";
        public const string RepairBounty      = "eco_repair_bounty_total";
        public const string CollectItems      = "eco_collect_for_payment_items_total";
        public const string CollectCurrency   = "eco_collect_for_payment_currency_total";
        public const string CraftingFees      = "eco_crafting_fees_total";
        public const string WorkOrderItems    = "eco_work_order_items_total";
        public const string Labor             = "eco_labor_total";
        public const string Garbage           = "eco_garbage_produced_total";
        public const string ContractCurrency  = "eco_contract_currency_total";
        public const string WorkPartyWork     = "eco_work_party_work_total";
        public const string LoanCurrency      = "eco_loan_currency_total";
        public const string LoanDefaulted     = "eco_loan_defaulted_total";
        public const string Tuition           = "eco_tuition_total";
        public const string StarsSpent        = "eco_stars_spent_total";
        public const string StarsRefunded     = "eco_stars_refunded_total";
        public const string XpRefunded        = "eco_xp_refunded_total";
        public const string Reputation        = "eco_reputation_transferred_total";
        public const string PollutionEmitted  = "eco_pollution_emitted_ppm_total";
        public const string PollutionRemoved  = "eco_pollution_removed_total";
        public const string DinnerGuests      = "eco_dinner_party_guests_total";
        public const string DinnerMeals       = "eco_dinner_party_meals_total";
        public const string DinnerCulture     = "eco_dinner_party_culture_total";
        public const string FoodEaten         = "eco_food_eaten_total";
        public const string FoodCalories      = "eco_food_calories_eaten_total";

        public static readonly IReadOnlyList<FamilyHelp> ValueHelp = new List<FamilyHelp>
        {
            new(Playtime,         MetricType.Counter, "Seconds a citizen was logged in, reported every 30 s."),
            new(Calories,         MetricType.Counter, "Calories spent performing actions."),
            new(TradeItems,       MetricType.Counter, "Items exchanged at stores, from the acting citizen's side."),
            new(TradeCurrency,    MetricType.Counter, "Currency exchanged at stores, from the acting citizen's side."),
            new(MoneyTransferred, MetricType.Counter, "Money transferred, by TransferType. Overlaps with the dedicated tax/rent/wage families; do not add them together."),
            new(TaxPaid,          MetricType.Counter, "Tax paid to a settlement treasury."),
            new(TaxBase,          MetricType.Counter, "Pre-tax amount of taxed transactions."),
            new(GovFunds,         MetricType.Counter, "Government money allocated to citizens."),
            new(RentPaid,         MetricType.Counter, "Move-in fees and rent paid."),
            new(WagesPaid,        MetricType.Counter, "Wages paid to elected officials."),
            new(CurrencyMinted,   MetricType.Counter, "Currency created at a mint."),
            new(CulturalSales,    MetricType.Counter, "Payment for cultural objects (paintings) sold."),
            new(RepairBounty,     MetricType.Counter, "Repair bounties paid out."),
            new(CollectItems,     MetricType.Counter, "Items collected from storage for payment."),
            new(CollectCurrency,  MetricType.Counter, "Currency paid for collected items."),
            new(CraftingFees,     MetricType.Counter, "Fees charged for work orders on public tables."),
            new(WorkOrderItems,   MetricType.Counter, "Items ordered when work orders were created."),
            new(Labor,            MetricType.Counter, "Labor points added to work orders."),
            new(Garbage,          MetricType.Counter, "Garbage produced as a crafting byproduct."),
            new(ContractCurrency, MetricType.Counter, "Contract payment amounts by stage."),
            new(WorkPartyWork,    MetricType.Counter, "Work contributed to work parties."),
            new(LoanCurrency,     MetricType.Counter, "Loan and bond principal by stage."),
            new(LoanDefaulted,    MetricType.Counter, "Amount defaulted on loans and bonds."),
            new(Tuition,          MetricType.Counter, "Tuition money by disposition."),
            new(StarsSpent,       MetricType.Counter, "Stars spent on specialties."),
            new(StarsRefunded,    MetricType.Counter, "Stars refunded by specialty resets."),
            new(XpRefunded,       MetricType.Counter, "XP refunded by specialty resets."),
            new(Reputation,       MetricType.Counter, "Reputation points given (absolute value)."),
            new(PollutionEmitted, MetricType.Counter, "CO2 released into the atmosphere by polluting objects (PPM)."),
            new(PollutionRemoved, MetricType.Counter, "Ground pollution removed by decontaminants."),
            new(DinnerGuests,     MetricType.Counter, "Guests at finished dinner parties."),
            new(DinnerMeals,      MetricType.Counter, "Meals eaten at finished dinner parties."),
            new(DinnerCulture,    MetricType.Counter, "Culture awarded by finished dinner parties."),
            new(FoodEaten,        MetricType.Counter, "Food items eaten by citizens. [Eco event Stomach.GlobalFoodEatenEvent]"),
            new(FoodCalories,     MetricType.Counter, "Calories of food eaten by citizens. [Eco event Stomach.GlobalFoodEatenEvent]"),
        };

        // ---- build ------------------------------------------------------------------------------

        /// <summary>
        /// Enumerates every non-abstract GameAction in every loaded assembly: <c>[NoStats]</c> types go to the filter set,
        /// catalogue types get their curated rule, everything else (other mods) an automatic rule with at most two labels.
        /// </summary>
        public static FamilyRuleSet Build(MetricsConfig config)
        {
            var playerFamilies = new HashSet<string>(config.PlayerLabelFamilies ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);
            var excluded       = new HashSet<string>(config.ExcludedFamilies    ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);
            var curated        = BuildCurated();
            var byType         = new Dictionary<Type, FamilyRule>();
            var noStats        = new HashSet<Type>();
            var help           = new List<FamilyHelp>();
            var warnings       = new List<string>();
            var familyOwners   = new Dictionary<string, Type>(StringComparer.Ordinal);

            foreach (var type in LoadedGameActionTypes())
            {
                if (type.IsDefined(typeof(NoStatsAttribute), inherit: true)) { noStats.Add(type); continue; }
                if (!curated.TryGetValue(type, out var rule))
                {
                    var family = MetricNames.ActionFamily(type.Name);
                    if (familyOwners.TryGetValue(family, out var owner) && owner != type)
                    {
                        var suffixed = family.Substring(0, family.Length - "_total".Length) + "_" + MetricNames.ToSnake(type.Assembly.GetName().Name ?? "mod") + "_total";
                        warnings.Add($"family '{family}' is claimed by {owner.FullName} and {type.FullName}; the latter is exported as '{suffixed}'.");
                        family = suffixed;
                    }
                    rule = AutoRule(type, family);
                }
                familyOwners[rule.Family] = type;
                byType[type] = rule;
                help.Add(new FamilyHelp(rule.Family, MetricType.Counter, CountHelp(type)));
            }
            return new FamilyRuleSet(byType, noStats, playerFamilies, excluded, help, warnings);
        }

        static IEnumerable<Type> LoadedGameActionTypes()
        {
            var result = new List<Type>();
            foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
            {
                if (asm.IsDynamic) continue;
                Type?[] types;
                try { types = asm.GetTypes(); }
                catch (ReflectionTypeLoadException ex) { types = ex.Types; }
                catch (Exception) { continue; }
                foreach (var t in types)
                    if (t != null && !t.IsAbstract && !t.IsGenericTypeDefinition && typeof(GameAction).IsAssignableFrom(t)) result.Add(t);
            }
            return result.OrderBy(t => t.FullName, StringComparer.Ordinal);
        }

        static string CountHelp(Type type)
        {
            var description = type.GetCustomAttributes<DescriptionAttribute>(inherit: false).FirstOrDefault()?.Description;
            var category    = type.GetCustomAttributes<CategoryAttribute>(inherit: false).FirstOrDefault()?.Category ?? "Uncategorized";
            return string.IsNullOrWhiteSpace(description)
                ? $"Eco action {type.Name} [category {category}]"
                : $"{description.Trim()} [Eco action {type.Name}, category {category}]";
        }

        // ---- helpers used by the rules (game thread; no reflection, no formatting) ----------------

        static Label L(string name, string value) => new(name, value);

        static CounterIncrement Inc(string family, double delta, Label[] labels, DeferredLabel[] deferred) => new(family, labels, deferred, delta);

        static DeferredLabel[] PlayerOnly(bool with, User? player) =>
            with && player != null ? new[] { new DeferredLabel(Lbl.Player, RefKind.Player, player.Id) } : NoDef;

        static DeferredLabel[] Cur(Currency? currency) =>
            currency == null ? NoDef : new[] { new DeferredLabel(Lbl.Currency, RefKind.Currency, currency.Id) };

        static DeferredLabel[] Sett(Settlement? settlement) =>
            settlement == null ? NoDef : new[] { new DeferredLabel(Lbl.Settlement, RefKind.Settlement, settlement.Id) };

        /// <summary>Player (when allowed), currency and settlement ids in one array; nulls are omitted, so a missing currency yields no label.</summary>
        static DeferredLabel[] Def(bool with, User? player, Currency? currency = null, Settlement? settlement = null)
        {
            var hasPlayer = with && player != null;
            var n = (hasPlayer ? 1 : 0) + (currency != null ? 1 : 0) + (settlement != null ? 1 : 0);
            if (n == 0) return NoDef;
            var r = new DeferredLabel[n];
            var i = 0;
            if (hasPlayer)          r[i++] = new DeferredLabel(Lbl.Player,     RefKind.Player,     player!.Id);
            if (currency != null)   r[i++] = new DeferredLabel(Lbl.Currency,   RefKind.Currency,   currency.Id);
            if (settlement != null) r[i]   = new DeferredLabel(Lbl.Settlement, RefKind.Settlement, settlement.Id);
            return r;
        }

        static DeferredLabel[] Ref(string name, RefKind kind, IHasID? obj) =>
            obj == null ? NoDef : new[] { new DeferredLabel(name, kind, obj.Id) };

        static string Tool(Item? tool)      => TypeLabels.Item(tool);
        static string Sp(Type? species)     => TypeLabels.Species(species);
        static string Sk(Item? skill)       => TypeLabels.Skill(skill);
        static string It(Item? item)        => TypeLabels.Item(item);
        static float  Cnt(AggregatableAction a) => a.Count;

        static Label[] ActionLabel(Type type) => new[] { L(Lbl.Action, MetricNames.ToSnake(type.Name.EndsWith("Action") && type.Name.Length > 6 ? type.Name[..^6] : type.Name)) };

        // ---- curated catalogue -------------------------------------------------------------------

        static Dictionary<Type, FamilyRule> BuildCurated()
        {
            var map = new Dictionary<Type, FamilyRule>();

            // The family string is computed once here and handed to the build lambda as 'f', so the per-event path never formats.
            void Add<T>(Func<T, bool, string, CounterIncrement[]> build) where T : GameAction
            {
                var family = MetricNames.ActionFamily(typeof(T).Name);
                map[typeof(T)] = new FamilyRule(typeof(T).Name, family, (a, p) => build((T)a, p, family), curated: true);
            }

            // Contract / Work Party
            void Contract<T>(string stage) where T : WorkableAction => Add<T>((a, p, f) => new[]
            {
                Inc(f, Cnt(a), NoLabels, Cur(a.Currency)),
                Inc(ContractCurrency, a.CurrencyAmount, new[] { L(Lbl.Stage, stage) }, Cur(a.Currency)),
            });
            Contract<PostedContract>("Posted");
            Contract<JoinedContract>("Joined");
            Contract<FailedContract>("Failed");
            Contract<CompletedContract>("Completed");

            void WorkParty<T>() where T : WorkableAction => Add<T>((a, p, f) => new[] { Inc(f, Cnt(a), NoLabels, Cur(a.Currency)) });
            WorkParty<PostedWorkParty>();
            WorkParty<JoinedWorkParty>();
            WorkParty<LeftWorkParty>();
            WorkParty<CompletedWorkParty>();
            Add<WorkedForWorkParty>((a, p, f) => new[]
            {
                Inc(f, Cnt(a), NoLabels, NoDef),
                Inc(WorkPartyWork, a.WorkContributed, NoLabels, NoDef),
            });

            // Finance
            void Loan<T>(string stage) where T : FinanceAction => Add<T>((a, p, f) =>
            {
                var kind = EnumLabel<LoanOrBond>.Of(a.LoanOrBond);
                return new[]
                {
                    Inc(f, Cnt(a), new[] { L(Lbl.Kind, kind) }, Cur(a.Currency)),
                    Inc(LoanCurrency, a.CurrencyAmount, new[] { L(Lbl.Kind, kind), L(Lbl.Stage, stage) }, Cur(a.Currency)),
                };
            });
            Loan<OfferedLoanOrBond>("Offered");
            Loan<AcceptedLoanOrBond>("Accepted");
            Loan<RepaidLoanOrBond>("Repaid");
            Add<DefaultedOnLoanOrBond>((a, p, f) =>
            {
                var kind = new[] { L(Lbl.Kind, EnumLabel<LoanOrBond>.Of(a.LoanOrBond)) };
                return new[]
                {
                    Inc(f, Cnt(a), kind, Cur(a.Currency)),
                    Inc(LoanDefaulted, a.DefaultedAmount, kind, Cur(a.Currency)),
                };
            });
            Add<PayRentOrMoveInFee>((a, p, f) =>
            {
                var op = new[] { L(Lbl.Op, EnumLabel<MoveInOrRentFee>.Of(a.MoveInOrRentFee)) };
                return new[]
                {
                    Inc(f, 1, op, Cur(a.Currency)),
                    Inc(RentPaid, a.CurrencyAmount, op, Cur(a.Currency)),
                };
            });
            Add<PayWages>((a, p, f) => new[]
            {
                Inc(f, 1, NoLabels, Def(false, null, a.Currency, a.Settlement)),
                Inc(WagesPaid, a.CurrencyAmount, NoLabels, Def(false, null, a.Currency, a.Settlement)),
            });

            // Economy
            Add<MintCurrency>((a, p, f) => new[]
            {
                Inc(f, Cnt(a), NoLabels, Cur(a.Currency)),
                Inc(CurrencyMinted, a.CurrencyAmount, NoLabels, Cur(a.Currency)),
            });
            Add<CreateCurrency>((a, p, f) => new[] { Inc(f, Cnt(a), NoLabels, NoDef) });
            Add<BankAccountPermissionsChanged>((a, p, f) => new[] { Inc(f, Cnt(a), NoLabels, NoDef) });
            Add<BarterTrade>((a, p, f) =>
            {
                var side = EnumLabel<BoughtOrSold>.Of(a.BoughtOrSold);
                var item = It(a.ItemUsed);
                var who  = PlayerOnly(p, a.Citizen);
                return new[]
                {
                    Inc(f, Cnt(a), new[] { L(Lbl.Side, side), L(Lbl.Item, item) }, who),
                    Inc(TradeItems, a.NumberOfItems, new[] { L(Lbl.Kind, "barter"), L(Lbl.Side, side), L(Lbl.Item, item) }, who),
                };
            });
            Add<CurrencyTrade>((a, p, f) =>
            {
                var side = EnumLabel<BoughtOrSold>.Of(a.BoughtOrSold);
                var item = It(a.ItemUsed);
                var who  = Def(p, a.Citizen, a.Currency);
                return new[]
                {
                    Inc(f, Cnt(a), new[] { L(Lbl.Side, side), L(Lbl.Item, item) }, who),
                    Inc(TradeItems, a.NumberOfItems, new[] { L(Lbl.Kind, "currency"), L(Lbl.Side, side), L(Lbl.Item, item) }, who),
                    Inc(TradeCurrency, a.CurrencyAmount, new[] { L(Lbl.Side, side), L(Lbl.Item, item) }, who),
                };
            });
            Add<CulturalObjectSold>((a, p, f) => new[]
            {
                Inc(f, Cnt(a), NoLabels, Cur(a.Currency)),
                Inc(CulturalSales, a.CurrencyAmount, NoLabels, Cur(a.Currency)),
            });
            Add<RepairBountyClaimed>((a, p, f) => new[]
            {
                Inc(f, Cnt(a), new[] { L(Lbl.Table, It(a.WorldObjectItem)) }, Cur(a.Currency)),
                Inc(RepairBounty, a.CurrencyAmount, NoLabels, Cur(a.Currency)),
            });
            Add<CollectForPayment>((a, p, f) =>
            {
                var item = It(a.ItemCollected);
                return new[]
                {
                    Inc(f, Cnt(a), new[] { L(Lbl.Item, item) }, Cur(a.Currency)),
                    Inc(CollectItems, a.Quantity, new[] { L(Lbl.Item, item) }, NoDef),
                    Inc(CollectCurrency, a.CurrencyAmount, NoLabels, Cur(a.Currency)),
                };
            });

            // Civics
            Add<DemographicChange>((a, p, f) => new[]
            {
                Inc(f, Cnt(a), new[] { L(Lbl.Op, EnumLabel<EnteredOrLeftDemographic>.Of(a.Entered)) }, Ref(Lbl.Demographic, RefKind.Demographic, a.Demographic)),
            });
            Add<PropertyTransfer>((a, p, f) => new[] { Inc(f, 1, NoLabels, NoDef) });
            Add<ClaimOrUnclaimProperty>((a, p, f) => new[]
            {
                Inc(f, 1, new[] { L(Lbl.Op, EnumLabel<ClaimedOrUnclaimed>.Of(a.ClaimedOrUnclaimed)) }, NoDef),
            });
            Add<ResidencyChanged>((a, p, f) => new[]
            {
                Inc(f, 1, new[]
                {
                    L(Lbl.Change, EnumLabel<ResidencyChange>.Of(a.ResidencyChange)),
                    L(Lbl.Reason, EnumLabel<ResidencyChangeReason>.Of(a.ResidencyChangeReason)),
                }, NoDef),
            });
            Add<TransferMoney>((a, p, f) =>
            {
                var reason = new[] { L(Lbl.Reason, EnumLabel<TransferType>.Of(a.Reason)) };
                return new[]
                {
                    Inc(f, Cnt(a), reason, Cur(a.Currency)),
                    Inc(MoneyTransferred, a.CurrencyAmount, reason, Cur(a.Currency)),
                };
            });
            Add<PayTax>((a, p, f) =>
            {
                var refs = Def(false, null, a.Currency, a.Settlement);
                return new[]
                {
                    Inc(f, Cnt(a), NoLabels, refs),
                    Inc(TaxPaid, a.CurrencyAmount, NoLabels, refs),
                    Inc(TaxBase, a.TotalBeforeTax, NoLabels, refs),
                };
            });
            Add<ReceiveGovernmentFunds>((a, p, f) => new[]
            {
                Inc(f, Cnt(a), NoLabels, Cur(a.Currency)),
                Inc(GovFunds, a.CurrencyAmount, NoLabels, Cur(a.Currency)),
            });
            Add<Vote>((a, p, f) => new[] { Inc(f, Cnt(a), new[] { L(Lbl.VoteType, EnumLabel<VoteType>.Of(a.VoteType)) }, NoDef) });
            Add<StartElection>((a, p, f) => new[] { Inc(f, Cnt(a), NoLabels, Ref(Lbl.Title, RefKind.ElectedTitle, a.ElectedTitle)) });
            Add<JoinOrLeaveElection>((a, p, f) => new[] { Inc(f, Cnt(a), new[] { L(Lbl.Op, EnumLabel<EnteredOrLeftElection>.Of(a.EnteredOrLeftElection)) }, NoDef) });
            Add<WonElection>((a, p, f) => new[] { Inc(f,  Cnt(a), NoLabels, NoDef) });
            Add<LostElection>((a, p, f) => new[] { Inc(f, Cnt(a), NoLabels, NoDef) });
            Add<DidntVote>((a, p, f) => new[] { Inc(f,    Cnt(a), NoLabels, NoDef) });
            Add<ReputationTransfer>((a, p, f) =>
            {
                var labels = new[]
                {
                    L(Lbl.Sign,   EnumLabel<PositiveOrNegativeRep>.Of(a.ReputationTransferredSign)),
                    L(Lbl.Source, EnumLabel<ReputationSource>.Of(a.ReputationSource)),
                };
                return new[]
                {
                    Inc(f, Cnt(a), labels, NoDef),
                    Inc(Reputation, Math.Abs(a.ReputationAmountTransferred), labels, NoDef),
                };
            });

            // Specialties
            Add<GainSpecialty>((a, p, f) =>
            {
                var who = PlayerOnly(p, a.Citizen);
                return new[]
                {
                    Inc(f, Cnt(a), new[] { L(Lbl.Specialty, Sk(a.Specialty)), L(Lbl.Method, EnumLabel<LearningMethod>.Of(a.MethodLearned)) }, who),
                    Inc(StarsSpent, a.StarsUsed, NoLabels, who),
                };
            });
            Add<LoseSpecialty>((a, p, f) => new[]
            {
                Inc(f, Cnt(a), new[] { L(Lbl.Specialty, Sk(a.Specialty)) }, PlayerOnly(p, a.Citizen)),
                Inc(StarsRefunded, a.StarsRefunded, NoLabels, NoDef),
                Inc(XpRefunded,    a.XPRefunded,    NoLabels, NoDef),
            });
            Add<GainProfession>((a, p, f) => new[] { Inc(f,    Cnt(a), new[] { L(Lbl.Profession, Sk(a.Profession)) }, PlayerOnly(p, a.Citizen)) });
            Add<SpecialtyLevelUp>((a, p, f) => new[] { Inc(f,  Cnt(a), new[] { L(Lbl.Specialty, Sk(a.Specialty)) },   PlayerOnly(p, a.Citizen)) });
            Add<CharacterLevelUp>((a, p, f) => new[] { Inc(f,  Cnt(a), NoLabels, PlayerOnly(p, a.Citizen)) });

            // Farming
            Add<FertilizeAction>((a, p, f) => new[] { Inc(f, Cnt(a), new[] { L(Lbl.Item, It(a.ItemUsed)) }, PlayerOnly(p, a.Citizen)) });
            Add<PlantSeeds>((a, p, f) => new[] { Inc(f,      Cnt(a), new[] { L(Lbl.Species, Sp(a.Species)) }, PlayerOnly(p, a.Citizen)) });

            // Harvesting
            var harvestAction = ActionLabel(typeof(HarvestOrHunt));
            Add<HarvestOrHunt>((a, p, f) =>
            {
                var who = PlayerOnly(p, a.Citizen);
                return new[]
                {
                    Inc(f, Cnt(a), new[] { L(Lbl.Species, Sp(a.Species)), L(Lbl.Tool, Tool(a.ToolUsed)) }, who),
                    Inc(Calories, a.CaloriesToConsume, harvestAction, who),
                };
            });
            var chopAction = ActionLabel(typeof(ChopTree));
            Add<ChopTree>((a, p, f) =>
            {
                var who = PlayerOnly(p, a.Citizen);
                return new[]
                {
                    Inc(f, Cnt(a), new[] { L(Lbl.Species, Sp(a.Species)), L(Lbl.Tool, Tool(a.ToolUsed)) }, who),
                    Inc(Calories, a.CaloriesToConsume, chopAction, who),
                };
            });
            Add<ChopStump>((a, p, f) => new[] { Inc(f,         Cnt(a), new[] { L(Lbl.Species, Sp(a.Species)) }, PlayerOnly(p, a.Citizen)) });
            Add<CleanupTreeDebris>((a, p, f) => new[] { Inc(f, Cnt(a), new[] { L(Lbl.Tool, Tool(a.ToolUsed)) },   PlayerOnly(p, a.Citizen)) });
            Add<CreateTreeDebris>((a, p, f) => new[] { Inc(f,  a.Count, NoLabels, PlayerOnly(p, a.Citizen)) }); // AccumulatableAction: Count is its own field

            // Crafting
            Add<CreateWorkOrder>((a, p, f) =>
            {
                var hasOrder = a.WorkOrder != null;
                var item     = hasOrder ? It(a.CraftedItem) : string.Empty;
                var table    = hasOrder ? It(a.WorldObjectItem) : string.Empty;
                return new[]
                {
                    Inc(f, Cnt(a), new[] { L(Lbl.Item, item), L(Lbl.Table, table) }, PlayerOnly(p, a.Citizen)),
                    Inc(WorkOrderItems, hasOrder ? a.OrderCount : 0, new[] { L(Lbl.Item, item) }, NoDef),
                    Inc(CraftingFees, a.CurrencyAmount, NoLabels, Cur(a.Currency)),
                };
            });
            Add<ItemCraftedAction>((a, p, f) => new[]
            {
                Inc(f, Cnt(a), new[] { L(Lbl.Item, It(a.ItemUsed)), L(Lbl.Table, It(a.WorldObjectItem)) }, PlayerOnly(p, a.Citizen)),
            });
            Add<ProduceGarbage>((a, p, f) =>
            {
                var table = new[] { L(Lbl.Table, It(a.WorldObjectItem)) };
                return new[]
                {
                    Inc(f, Cnt(a), table, PlayerOnly(p, a.Citizen)),
                    Inc(Garbage, a.Amount, table, NoDef),
                };
            });
            Add<LaborWorkOrderAction>((a, p, f) =>
            {
                var hasOrder  = a.WorkOrder != null;
                var specialty = hasOrder ? Sk(a.LaborSkill) : string.Empty;
                var table     = hasOrder ? It(a.WorldObjectItem) : string.Empty;
                return new[]
                {
                    Inc(f, Cnt(a), new[] { L(Lbl.Specialty, specialty), L(Lbl.Table, table) }, PlayerOnly(p, a.Citizen)),
                    Inc(Labor, a.LaborAdded, new[] { L(Lbl.Specialty, specialty) }, NoDef),
                };
            });

            // Painting, road work and other tool actions: {tool} + calories
            void ToolAction<T>() where T : ToolInteractAction
            {
                var family = MetricNames.ActionFamily(typeof(T).Name);
                var action = ActionLabel(typeof(T));
                Add<T>((a, p, f) =>
                {
                    var who = PlayerOnly(p, a.Citizen);
                    return new[]
                    {
                        Inc(family, Cnt(a), new[] { L(Lbl.Tool, Tool(a.ToolUsed)) }, who),
                        Inc(Calories, a.CaloriesToConsume, action, who),
                    };
                });
            }
            ToolAction<BlockPaint>();
            ToolAction<ObjectPaint>();
            ToolAction<BlockPaintCleanup>();
            ToolAction<ObjectPaintCleanup>();
            ToolAction<TampRoad>();
            ToolAction<PlowField>();

            // Mining
            Add<ObjectExplosion>((a, p, f) => new[] { Inc(f, Cnt(a), NoLabels, PlayerOnly(p, a.Citizen)) });

            // Construction
            var constructAction = ActionLabel(typeof(ConstructOrDeconstruct));
            Add<ConstructOrDeconstruct>((a, p, f) =>
            {
                var who = PlayerOnly(p, a.Citizen);
                return new[]
                {
                    Inc(f, Cnt(a), new[] { L(Lbl.Op, EnumLabel<ConstructedOrDeconstructed>.Of(a.ConstructedOrDeconstructed)), L(Lbl.Block, It(a.ItemUsed)) }, who),
                    Inc(Calories, a.CaloriesToConsume, constructAction, who),
                };
            });
            var digAction = ActionLabel(typeof(DigOrMine));
            Add<DigOrMine>((a, p, f) =>
            {
                var who = PlayerOnly(p, a.Citizen);
                return new[]
                {
                    Inc(f, Cnt(a), new[] { L(Lbl.Block, It(a.ItemUsed)), L(Lbl.Tool, Tool(a.ToolUsed)) }, who),
                    Inc(Calories, a.CaloriesToConsume, digAction, who),
                };
            });
            Add<DropOrPickupBlock>((a, p, f) => new[]
            {
                Inc(f, Cnt(a), new[] { L(Lbl.Op, EnumLabel<DroppedOrPickedUp>.Of(a.DroppedOrPickedUp)), L(Lbl.Block, It(a.ItemUsed)) }, PlayerOnly(p, a.Citizen)),
            });
            Add<PlaceOrPickUpObject>((a, p, f) => new[]
            {
                Inc(f, Cnt(a), new[] { L(Lbl.Op, EnumLabel<PlacedOrPickedUp>.Of(a.PlacedOrPickedUp)), L(Lbl.Object, It(a.WorldObjectItem ?? a.ItemUsed)) }, PlayerOnly(p, a.Citizen)),
            });
            Add<MoveWorldObject>((a, p, f) => new[] { Inc(f, Cnt(a), new[] { L(Lbl.Object, It(a.WorldObjectItem ?? a.ItemUsed)) }, PlayerOnly(p, a.Citizen)) });

            // Citizens
            Add<ChatSent>((a, p, f) => new[] { Inc(f, Cnt(a), NoLabels, PlayerOnly(p, a.Citizen)) });
            Add<Play>((a, p, f) =>
            {
                var who = PlayerOnly(p, a.Citizen);
                return new[]
                {
                    Inc(f, Cnt(a), NoLabels, who),
                    Inc(Playtime, a.SecondsPassed, NoLabels, who),
                };
            });
            Add<FirstLogin>((a, p, f) => new[] { Inc(f, Cnt(a), NoLabels, PlayerOnly(p, a.Citizen)) });

            // Pollution
            Add<PolluteAir>((a, p, f) =>
            {
                var source = new[] { L(Lbl.Source, It(a.PollutionSource)) };
                return new[]
                {
                    Inc(f, 1, source, NoDef), // Count is overridden to PollutionInPPM
                    Inc(PollutionEmitted, a.PollutionInPPM, source, NoDef),
                };
            });
            Add<DropOrPickupGarbage>((a, p, f) => new[]
            {
                Inc(f, Cnt(a), new[] { L(Lbl.Op, EnumLabel<DroppedOrPickedUp>.Of(a.DroppedOrPickedUp)) }, PlayerOnly(p, a.Citizen)),
            });
            Add<Decontaminate>((a, p, f) =>
            {
                var item = new[] { L(Lbl.Item, It(a.ItemUsed)) };
                return new[]
                {
                    Inc(f, Cnt(a), item, PlayerOnly(p, a.Citizen)),
                    Inc(PollutionRemoved, a.PollutionRemoved, item, NoDef),
                };
            });

            // Education (Student/Teacher are never labels)
            Add<EnrollAction>((a, p, f) => new[]
            {
                Inc(f, Cnt(a), new[] { L(Lbl.Specialty, Sk(a.SubjectBeingTaught)) }, Cur(a.TuitionCurrency)),
                Inc(Tuition, a.TuitionFee, new[] { L(Lbl.Part, "Enrolled") }, Cur(a.TuitionCurrency)),
            });
            CounterIncrement[] TuitionParts(LeaveClassBase a, CounterIncrement head) => new[]
            {
                head,
                Inc(Tuition, a.TuitionFeePaidToTeacher, new[] { L(Lbl.Part, "Teacher") },  Cur(a.TuitionCurrency)),
                Inc(Tuition, a.TuitionFeePaidToOwner,   new[] { L(Lbl.Part, "Owner") },    Cur(a.TuitionCurrency)),
                Inc(Tuition, a.TuitionFeeRefunded,      new[] { L(Lbl.Part, "Refunded") }, Cur(a.TuitionCurrency)),
            };
            Add<CompleteClass>((a, p, f) => TuitionParts(a, Inc(f, Cnt(a), NoLabels, Cur(a.TuitionCurrency))));
            Add<LeftClass>((a, p, f) => TuitionParts(a, Inc(f, Cnt(a), new[] { L(Lbl.Reason, EnumLabel<StoppedStudying>.Of(a.CompletionReason)) }, Cur(a.TuitionCurrency))));

            // Dinner parties
            Add<DinnerPartyStarted>((a, p, f) => new[] { Inc(f,       1, NoLabels, PlayerOnly(p, a.Citizen)) });
            Add<ParticipateInDinnerParty>((a, p, f) => new[] { Inc(f, 1, NoLabels, PlayerOnly(p, a.Citizen)) });
            Add<EatFoodInDinnerParty>((a, p, f) => new[] { Inc(f,     1, new[] { L(Lbl.Item, It(a.ItemUsed)) }, PlayerOnly(p, a.Citizen)) });
            Add<DinnerPartyEnded>((a, p, f) => new[]
            {
                Inc(f, 1, NoLabels, NoDef),
                Inc(DinnerGuests,  a.GuestCount,   NoLabels, NoDef),
                Inc(DinnerMeals,   a.MealsCount,   NoLabels, NoDef),
                Inc(DinnerCulture, a.CultureTotal, NoLabels, NoDef),
            });

            // Settlements
            Add<PlaceNewSettlementFoundation>((a, p, f) => new[] { Inc(f, Cnt(a), new[] { L(Lbl.SettlementType, It(a.SettlementType)) }, PlayerOnly(p, a.Citizen)) });
            Add<SettlementFounded>((a, p, f) => new[] { Inc(f,            Cnt(a), new[] { L(Lbl.SettlementType, It(a.SettlementType)) }, Def(p, a.Citizen, null, a.Settlement)) });
            Add<StartHomestead>((a, p, f) => new[] { Inc(f,               Cnt(a), NoLabels, PlayerOnly(p, a.Citizen)) });
            Add<BecomeCitizen>((a, p, f) => new[] { Inc(f,                1, new[] { L(Lbl.FirstTime, EnumLabel<FirstTimeOrNotFirstTime>.Of(a.FirstTimeJoining)) }, Def(p, a.Citizen, null, a.Settlement)) });
            Add<LeaveCitizenship>((a, p, f) => new[] { Inc(f,             1, NoLabels, Def(p, a.Citizen, null, a.Settlement)) });

            return map;
        }

        // ---- automatic rule for unknown (modded) GameAction types --------------------------------

        static readonly Dictionary<string, string> AutoRenames = new(StringComparer.Ordinal)
        {
            ["BoughtOrSold"] = Lbl.Side,   ["LoanOrBond"] = Lbl.Kind,          ["ItemUsed"] = Lbl.Item,       ["CraftedItem"] = Lbl.Item,
            ["ItemCollected"] = Lbl.Item,  ["ToolUsed"] = Lbl.Tool,            ["WorldObjectItem"] = Lbl.Object, ["Species"] = Lbl.Species,
            ["Reason"] = Lbl.Reason,       ["ResidencyChange"] = Lbl.Change,   ["ResidencyChangeReason"] = Lbl.Reason,
            ["ReputationTransferredSign"] = Lbl.Sign, ["ReputationSource"] = Lbl.Source, ["PollutionSource"] = Lbl.Source,
            ["SettlementType"] = Lbl.SettlementType, ["VoteType"] = Lbl.VoteType, ["MethodLearned"] = Lbl.Method, ["CompletionReason"] = Lbl.Reason,
        };

        sealed class AutoLabel
        {
            public readonly string                    Name;
            public readonly Func<GameAction, object?> Get;
            public readonly bool                      IsEnum;
            public readonly bool                      IsType;
            public readonly ConcurrentDictionary<object, string> EnumNames = new();
            public AutoLabel(string name, Func<GameAction, object?> get, bool isEnum, bool isType) { this.Name = name; this.Get = get; this.IsEnum = isEnum; this.IsType = isType; }
        }

        /// <summary>At most two labels: enums first, then Item/Type properties, in declaration order; never player.</summary>
        static FamilyRule AutoRule(Type type, string family)
        {
            var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                            .Where(p => p.CanRead && p.GetIndexParameters().Length == 0 && p.DeclaringType != typeof(GameAction) && p.Name != "Count")
                            .OrderBy(p => Depth(p.DeclaringType!)).ThenBy(p => p.MetadataToken)
                            .ToList();
            var picked = new List<AutoLabel>(2);
            var names  = new HashSet<string>(StringComparer.Ordinal);
            foreach (var p in props.Where(p => p.PropertyType.IsEnum))
                if (picked.Count < 2 && names.Add(AutoLabelName(p))) picked.Add(new AutoLabel(AutoLabelName(p), Getter(type, p), true, false));
            foreach (var p in props.Where(p => typeof(Item).IsAssignableFrom(p.PropertyType) || p.PropertyType == typeof(Type)))
                if (picked.Count < 2 && names.Add(AutoLabelName(p))) picked.Add(new AutoLabel(AutoLabelName(p), Getter(type, p), false, p.PropertyType == typeof(Type)));

            var countProp      = type.GetProperty("Count", BindingFlags.Public | BindingFlags.Instance);
            var countOverridden = countProp != null && countProp.DeclaringType != typeof(AggregatableAction);
            var labels          = picked.ToArray();

            return new FamilyRule(type.Name, family, (action, _) =>
            {
                var delta = countOverridden ? 1.0 : (action as IAggregatableStat)?.Count ?? 1.0;
                Label[] ls;
                if (labels.Length == 0) ls = NoLabels;
                else
                {
                    ls = new Label[labels.Length];
                    for (int i = 0; i < labels.Length; i++) ls[i] = new Label(labels[i].Name, AutoValue(labels[i], action));
                }
                return new[] { new CounterIncrement(family, ls, NoDef, delta) };
            }, curated: false);
        }

        static string AutoValue(AutoLabel label, GameAction action)
        {
            object? v;
            try { v = label.Get(action); }
            catch (Exception) { return string.Empty; }
            if (v == null) return string.Empty;
            if (label.IsEnum) return label.EnumNames.GetOrAdd(v, static o => o.ToString() ?? string.Empty);
            if (label.IsType) return TypeLabels.Species((Type)v);
            return v is Item item ? TypeLabels.Item(item) : string.Empty;
        }

        static string AutoLabelName(PropertyInfo p)
        {
            if (AutoRenames.TryGetValue(p.Name, out var renamed)) return renamed;
            if (p.PropertyType.IsEnum && p.PropertyType.Name.Contains("Or")) return Lbl.Op;
            return MetricNames.LabelName(p.Name);
        }

        static Func<GameAction, object?> Getter(Type type, PropertyInfo p)
        {
            var param = Expression.Parameter(typeof(GameAction), "a");
            var body  = Expression.Convert(Expression.Property(Expression.Convert(param, type), p), typeof(object));
            return Expression.Lambda<Func<GameAction, object?>>(body, param).Compile();
        }

        static int Depth(Type t) { var d = 0; for (var b = t.BaseType; b != null; b = b.BaseType) d++; return d; }
    }
}
