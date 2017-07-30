using Akka.Configuration;
using Akka.Persistence.Azure.Tables;
using Akka.Persistence.Azure.Tables.Tests;
using Akka.Persistence.TCK.Snapshot;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.AzureTable.Tests
{
    [Collection("AzureTableSpec")]
    public class AzureTableSnapshotStoreSpec : SnapshotStoreSpec
    {
        private static readonly Config SpecConfig;
        private static readonly string ConnectionString;
        private static readonly string TableName;

        static AzureTableSnapshotStoreSpec()
        {
#if CI
            var connectionString = Environment.GetEnvironmentVariable("ConnectionString");
#else
            var connectionString = "UseDevelopmentStorage=true;";
#endif

            SpecConfig = ConfigurationFactory.ParseString(@"
                akka.test.single-expect-default = 10s
                akka.persistence {
                    publish-plugin-commands = on
                    snapshot-store.plugin = ""akka.persistence.snapshot-store.azure-table""
                    snapshot-store.azure-table.connection-string = """ + connectionString + @"""
                    snapshot-store.azure-table.auto-initialize = on
                    snapshot-store.azure-table.table-name = snapshots
                }");

            ConnectionString = SpecConfig.GetString("akka.persistence.snapshot-store.azure-table.connection-string");
            TableName = SpecConfig.GetString("akka.persistence.snapshot-store.azure-table.table-name");
        }

        public AzureTableSnapshotStoreSpec(ITestOutputHelper output)
            : base(SpecConfig, typeof(AzureTableJournalSpec).Name, output)
        {
            DbUtils.CleanAsync(ConnectionString, TableName).Wait();

            AzureTablePersistence.Get(Sys);
            Initialize();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.CleanAsync(ConnectionString, TableName).Wait();
        }
    }
}