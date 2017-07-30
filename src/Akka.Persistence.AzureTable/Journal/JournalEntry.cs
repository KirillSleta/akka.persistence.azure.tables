using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.Azure.Tables.Journal
{
    /// <summary>
    ///     Class used for storing intermediate result of the <see cref="IPersistentRepresentation" />
    /// </summary>
    public class JournalEntry : TableEntity
    {
        public JournalEntry()
        {
        }

        public JournalEntry(string persistenceId, long sequenceNr, byte[] payload, string manifest)
        {
            PartitionKey = persistenceId;
            RowKey = ToRowKey(sequenceNr);

            Payload = payload;
            Manifest = manifest;
        }

        public byte[] Payload { get; set; }

        public string Manifest { get; set; }

        public static string ToRowKey(long version)
        {
            return version.ToString().PadLeft(10, '0');
        }
    }
}