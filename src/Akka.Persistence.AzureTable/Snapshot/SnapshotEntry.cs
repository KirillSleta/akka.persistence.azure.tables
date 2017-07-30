using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.Azure.Tables.Snapshot
{
    public class SnapshotEntry : TableEntity
    {
        public SnapshotEntry()
        {
        }

        public SnapshotEntry(string persistenceId, long sequenceNr, long snapshotTimestamp,
            byte[] payload)
        {
            PartitionKey = persistenceId;
            RowKey = ToRowKey(sequenceNr);

            SnapshotTimestamp = snapshotTimestamp;
            Payload = payload;
        }

        public long SnapshotTimestamp { get; set; }

        public byte[] Payload { get; set; }

        public static string ToRowKey(long version)
        {
            return version.ToString().PadLeft(10, '0');
        }
    }
}