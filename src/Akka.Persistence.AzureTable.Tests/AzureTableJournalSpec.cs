//-----------------------------------------------------------------------
// <copyright file="AzureTableJournalSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;
using Akka.Persistence.TCK.Journal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Azure.Tables.Tests
{
    [Collection("AzureTableSpec")]
    public class AzureTableJournalSpec : JournalSpec
    {
        private static readonly Config SpecConfig;
        private static readonly string ConnectionString;
        private static readonly string TableName;
        private static readonly string MetadataTableName;

        static AzureTableJournalSpec()
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
                    journal.plugin = ""akka.persistence.journal.azure-table""
                    journal.azure-table.connection-string = """ + connectionString + @"""
                    journal.azure-table.auto-initialize = on
                    journal.azure-table.table-name = events
                    journal.azure-table.metadata-table-name = metadata
                }");

            ConnectionString = SpecConfig.GetString("akka.persistence.journal.azure-table.connection-string");
            TableName = SpecConfig.GetString("akka.persistence.journal.azure-table.table-name");
            MetadataTableName = SpecConfig.GetString("akka.persistence.journal.azure-table.metadata-table-name");
        }

        public AzureTableJournalSpec(ITestOutputHelper output)
            : base(SpecConfig, typeof(AzureTableJournalSpec).Name, output)
        {
            DbUtils.CleanAsync(ConnectionString, TableName).Wait();
            DbUtils.CleanAsync(ConnectionString, MetadataTableName).Wait();

            AzureTablePersistence.Get(Sys);
            Initialize();
        }

        protected override bool SupportsRejectingNonSerializableObjects { get; } = false;

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DbUtils.CleanAsync(ConnectionString, TableName).Wait();
            DbUtils.CleanAsync(ConnectionString, MetadataTableName).Wait();
        }
    }
}