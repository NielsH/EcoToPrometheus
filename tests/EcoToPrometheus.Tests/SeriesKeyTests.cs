namespace EcoToPrometheus.Tests
{
    using System;
    using EcoToPrometheus.Core;
    using Xunit;

    public class SeriesKeyTests
    {
        [Fact]
        public void Empty_labels_is_just_the_family()
        {
            var key = SeriesKey.Build("eco_x_total", Array.Empty<Label>());
            Assert.Equal("eco_x_total", key);
            var (family, labels) = SeriesKey.Parse(key);
            Assert.Equal("eco_x_total", family);
            Assert.Empty(labels);
        }

        [Fact]
        public void Plain_labels_round_trip_in_order()
        {
            var labels = new[] { new Label("item", "IronPickaxe"), new Label("player", "Ann") };
            var key = SeriesKey.Build("eco_x_total", labels);
            Assert.Equal("eco_x_total|item=IronPickaxe|player=Ann", key);
            var (family, parsed) = SeriesKey.Parse(key);
            Assert.Equal("eco_x_total", family);
            Assert.Equal(labels, parsed);
        }

        [Theory]
        [InlineData("a|b")]
        [InlineData("a=b")]
        [InlineData("back\\slash")]
        [InlineData("trailing\\")]
        [InlineData("\\|=\\|=")]
        [InlineData("Zoë Ångström 玩家 🙂")]
        [InlineData("")]
        [InlineData("with\nnewline and \"quotes\"")]
        public void Awkward_values_round_trip(string value)
        {
            var labels = new[] { new Label("player", value), new Label("z", "1") };
            var key = SeriesKey.Build("fam", labels);
            var (family, parsed) = SeriesKey.Parse(key);
            Assert.Equal("fam", family);
            Assert.Equal(labels, parsed);
        }

        [Fact]
        public void Awkward_label_names_round_trip()
        {
            var labels = new[] { new Label("we|rd=name", "v") };
            var (_, parsed) = SeriesKey.Parse(SeriesKey.Build("fam", labels));
            Assert.Equal(labels, parsed);
        }

        [Fact]
        public void Escaped_separators_do_not_split()
        {
            var key = SeriesKey.Build("fam", new[] { new Label("a", "x|y"), new Label("b", "p=q") });
            var (_, parsed) = SeriesKey.Parse(key);
            Assert.Equal(2, parsed.Length);
            Assert.Equal("x|y", parsed[0].Value);
            Assert.Equal("p=q", parsed[1].Value);
        }

        [Theory]
        [InlineData("fam|noequals")]
        [InlineData("fam|a=b=c")]
        [InlineData("fam|a=b|")]
        public void Parse_rejects_malformed_segment(string key)
        {
            Assert.Throws<FormatException>(() => SeriesKey.Parse(key));
        }
    }
}
