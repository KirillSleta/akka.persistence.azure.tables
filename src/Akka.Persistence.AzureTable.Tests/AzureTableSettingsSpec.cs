using System;
using FluentAssertions;
using Xunit;

namespace Akka.Persistence.Azure.Tables.Tests
{
    [Collection("AzureTableSpec")]
    public class AzureTableSettingsSpec : Akka.TestKit.Xunit2.TestKit
    {
        [Fact]
        public void AzureTable_AzureTableSettings_must_throw_an_error_on_empty_config()
        {
            Assert.Throws<ArgumentNullException>(() => AzureTableJournalSettings.Create(null));
            Assert.Throws<ArgumentNullException>(() => AzureTableSnapshotStoreSettings.Create(null));
        }

        [Fact]
        public void AzureTable_JournalSettings_must_have_default_values()
        {
            var azurePersistence = AzureTablePersistence.Get(Sys);

            azurePersistence.JournalSettings.ConnectionString.Should().Be("UseDevelopmentStorage=true");
            azurePersistence.JournalSettings.TableName.Should().Be("events");
            azurePersistence.JournalSettings.MetadataTableName.Should().Be("metadata");
            azurePersistence.JournalSettings.AutoInitialize.Should().BeTrue();
        }

        [Fact]
        public void AzureTable_SnapshotStoreSettings_must_have_default_values()
        {
            var azurePersistence = AzureTablePersistence.Get(Sys);

            azurePersistence.SnapshotStoreSettings.ConnectionString.Should().Be("UseDevelopmentStorage=true");
            azurePersistence.SnapshotStoreSettings.TableName.Should().Be("snapshots");
            azurePersistence.SnapshotStoreSettings.AutoInitialize.Should().BeTrue();
        }
    }
}