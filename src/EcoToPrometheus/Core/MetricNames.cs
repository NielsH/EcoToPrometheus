namespace EcoToPrometheus.Core
{
    using System.Text;
    using System.Text.RegularExpressions;

    /// <summary>Deterministic naming rules from the spec. Pure functions, unit-tested in phase 2.</summary>
    public static class MetricNames
    {
        static readonly Regex LowerUpper   = new("([a-z0-9])([A-Z])", RegexOptions.Compiled);
        static readonly Regex AcronymWord  = new("([A-Z]+)([A-Z][a-z])", RegexOptions.Compiled);
        static readonly Regex Invalid      = new("[^a-z0-9_]", RegexOptions.Compiled);
        static readonly Regex Underscores  = new("_+", RegexOptions.Compiled);

        /// <summary>CamelCase type or field name to snake_case: <c>HarvestOrHunt</c> -> <c>harvest_or_hunt</c>, <c>CO2Scrubber</c> -> <c>co2_scrubber</c>.</summary>
        public static string ToSnake(string name)
        {
            var s = LowerUpper.Replace(name, "$1_$2");
            s = AcronymWord.Replace(s, "$1_$2");
            s = s.ToLowerInvariant();
            s = Invalid.Replace(s, "_");
            s = Underscores.Replace(s, "_").Trim('_');
            return s.Length == 0 ? "_" : s;
        }

        /// <summary>GameAction class name to its count family: <c>ItemCraftedAction</c> -> <c>eco_action_item_crafted_total</c>.</summary>
        public static string ActionFamily(string typeName)
        {
            if (typeName.EndsWith("Action") && typeName.Length > 6) typeName = typeName.Substring(0, typeName.Length - 6);
            return "eco_action_" + ToSnake(typeName) + "_total";
        }

        /// <summary>Item/species/skill type name to a label value: one trailing <c>Item</c>, <c>Species</c> or <c>Skill</c> stripped.</summary>
        public static string TypeLabel(string typeName, string suffix)
        {
            if (typeName.Length > suffix.Length && typeName.EndsWith(suffix)) return typeName.Substring(0, typeName.Length - suffix.Length);
            return typeName;
        }

        /// <summary>Field name to label name after snake-casing, e.g. <c>ToolUsed</c> -> <c>tool_used</c>. Renames live in FamilyRules.</summary>
        public static string LabelName(string fieldName) => ToSnake(fieldName);

        /// <summary>Makes an arbitrary string a valid metric name (used for unknown/modded types after ToSnake).</summary>
        public static string EnsureValidMetricName(string name)
        {
            var sb = new StringBuilder(name.Length + 1);
            if (name.Length == 0 || char.IsDigit(name[0])) sb.Append('_');
            foreach (var ch in name) sb.Append(char.IsAsciiLetterOrDigit(ch) || ch == '_' || ch == ':' ? ch : '_');
            return sb.ToString();
        }
    }
}
