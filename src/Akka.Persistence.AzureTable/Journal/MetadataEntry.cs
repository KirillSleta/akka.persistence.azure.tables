using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.Azure.Tables.Journal
{
    public class MetadataEntry : TableEntity
    {
        public MetadataEntry()
        {
        }

        public MetadataEntry(string persistenceId, long highestSequenceNr)
        {
            PartitionKey = persistenceId;
            RowKey = persistenceId;

            HighestSequenceNr = highestSequenceNr;
        }

        public long HighestSequenceNr { get; set; }
    }
}