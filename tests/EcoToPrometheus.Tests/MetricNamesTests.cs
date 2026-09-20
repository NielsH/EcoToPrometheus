namespace EcoToPrometheus.Tests
{
    using EcoToPrometheus.Core;
    using Xunit;

    public class MetricNamesTests
    {
        [Theory]
        [InlineData("HarvestOrHunt",     "eco_action_harvest_or_hunt_total")]
        [InlineData("ItemCraftedAction", "eco_action_item_crafted_total")]
        [InlineData("PolluteAir",        "eco_action_pollute_air_total")]
        [InlineData("CO2ScrubberAction", "eco_action_co2_scrubber_total")]
        [InlineData("DinnerPartyEnded",  "eco_action_dinner_party_ended_total")]
        [InlineData("ChopTree",          "eco_action_chop_tree_total")]
        [InlineData("Action",            "eco_action_action_total")]
        public void ActionFamily_snake_cases_and_strips_Action(string typeName, string expected)
        {
            Assert.Equal(expected, MetricNames.ActionFamily(typeName));
        }

        [Theory]
        [InlineData("HarvestOrHunt", "harvest_or_hunt")]
        [InlineData("CO2Scrubber",   "co2_scrubber")]
        [InlineData("ToolUsed",      "tool_used")]
        [InlineData("XMLParser",     "xml_parser")]
        [InlineData("already_snake", "already_snake")]
        [InlineData("With-Dash",     "with_dash")]
        [InlineData("",              "_")]
        public void ToSnake_table(string input, string expected)
        {
            Assert.Equal(expected, MetricNames.ToSnake(input));
        }

        [Theory]
        [InlineData("IronPickaxeItem", "Item",    "IronPickaxe")]
        [InlineData("OakSpecies",      "Species", "Oak")]
        [InlineData("LoggingSkill",    "Skill",   "Logging")]
        [InlineData("Item",            "Item",    "Item")]
        [InlineData("ItemItem",        "Item",    "Item")]
        [InlineData("Bread",           "Item",    "Bread")]
        public void TypeLabel_strips_one_suffix(string typeName, string suffix, string expected)
        {
            Assert.Equal(expected, MetricNames.TypeLabel(typeName, suffix));
        }

        [Theory]
        [InlineData("9abc-d",   "_9abc_d")]
        [InlineData("ok_name",  "ok_name")]
        [InlineData("ns:name",  "ns:name")]
        [InlineData("",         "_")]
        [InlineData("a b.c",    "a_b_c")]
        public void EnsureValidMetricName_table(string input, string expected)
        {
            Assert.Equal(expected, MetricNames.EnsureValidMetricName(input));
        }

        [Fact]
        public void LabelName_is_snake_case()
        {
            Assert.Equal("tool_used", MetricNames.LabelName("ToolUsed"));
        }
    }
}
