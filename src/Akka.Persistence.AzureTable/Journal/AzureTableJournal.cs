using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence.Journal;
using Akka.Serialization;
using Akka.Util.Internal;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.Azure.Tables.Journal
{
    public class AzureTableJournal : AsyncWriteJournal
    {
        private static readonly object Ack = new object();
        private static readonly Type PersistentRepresentationType = typeof(IPersistentRepresentation);
        private readonly Serializer _serializer;
        private readonly AzureTableJournalSettings _settings;
        private Lazy<CloudTableClient> _client;

        public AzureTableJournal()
        {
            _settings = AzureTablePersistence.Get(Context.System).JournalSettings;
            _serializer = Context.System.Serialization.FindSerializerForType(PersistentRepresentationType);
        }

        protected override void PreStart()
        {
            base.PreStart();
            _client = new Lazy<CloudTableClient>(() => CloudStorageAccount.Parse(_settings.ConnectionString)
                .CreateCloudTableClient());
            if (_settings.AutoInitialize)
            {
                _client.Value.GetTableReference(_settings.TableName).CreateIfNotExistsAsync().Wait();
                _client.Value.GetTableReference(_settings.MetadataTableName).CreateIfNotExistsAsync().Wait();
            }
        }

        /// <summary>
        ///     Asynchronously replays persistent messages.
        /// </summary>
        /// <param name="context">The contextual information about the actor processing replayed messages.</param>
        /// <param name="persistenceId">Persistent actor identifier</param>
        /// <param name="fromSequenceNr">Inclusive sequence number where replay should start</param>
        /// <param name="toSequenceNr">Inclusive sequence number where replay should end</param>
        /// <param name="max">Maximum number of messages to be replayed</param>
        /// <param name="recoveryCallback">Called to replay a message, may be called from any thread.</param>
        public override Task ReplayMessagesAsync(
            IActorContext context,
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            var dispatcher = Context.System.Dispatchers.DefaultGlobalDispatcher;
            var promise = new TaskCompletionSource<object>();
            dispatcher.Schedule(async () =>
            {
                try
                {
                    if (max == 0)
                    {
                        promise.SetResult(Ack);
                        return;
                    }
                    var table = _client.Value.GetTableReference(_settings.TableName);
                    var tableQuery = BuildReplayTableQuery(persistenceId, fromSequenceNr, toSequenceNr);

                    await table.QuerySegmentedWithCallback(tableQuery,
                        e => recoveryCallback(ToPersistenceRepresentation(e)), max).ConfigureAwait(false);

                    promise.SetResult(Ack);
                }
                catch (Exception e)
                {
                    promise.SetException(e);
                }
            });

            return promise.Task;
        }

        /// <summary>
        ///     Asynchronously reads a highest sequence number of the event stream related with provided
        ///     <paramref name="persistenceId" />.
        /// </summary>
        /// <param name="persistenceId">Persistent actor identifier</param>
        /// <param name="fromSequenceNr">Hint where to start searching for the highest sequence number</param>
        public override Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            var table = _client.Value.GetTableReference(_settings.MetadataTableName);

            var tableResult =
                table.ExecuteAsync(TableOperation.Retrieve<MetadataEntry>(persistenceId, persistenceId)).Result;

            return Task.FromResult(tableResult.HttpStatusCode != 200
                ? 0L
                : tableResult.Result.AsInstanceOf<MetadataEntry>().HighestSequenceNr);
        }

        /// <summary>
        ///     Asynchronously deletes all persistent messages up to inclusive <paramref name="toSequenceNr" /> bound.
        /// </summary>
        /// <param name="persistenceId">Persistent actor identifier</param>
        /// <param name="toSequenceNr">Highest sequence number to delete</param>
        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            var table = _client.Value.GetTableReference(_settings.TableName);
            var query = BuildDeleteTableQuery(persistenceId, toSequenceNr);

            async void Callback(IEnumerable<JournalEntry> results)
            {
                await table.Execute(results, (batch, entity) => batch.Delete(entity));
            }

            await table.QuerySegmentedWithCallback(query,
                Callback);
        }

        /// <summary>
        ///     Asynchronously writes a batch of persistent messages to the journal.
        /// </summary>
        /// <param name="messages">The atomic messages.</param>
        /// <returns></returns>
        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            var table = _client.Value.GetTableReference(_settings.TableName);

            var messagesList = messages.ToList();
            var groupedTasks = messagesList.GroupBy(x => x.PersistenceId).ToDictionary(g => g.Key, async g =>
            {
                var persistentMessages = g.SelectMany(aw => (IImmutableList<IPersistentRepresentation>) aw.Payload)
                    .ToList();

                var persistenceId = g.Key;
                var highSequenceId = persistentMessages.Max(c => c.SequenceNr);

                await table.Execute(persistentMessages.Select(ToJournalEntry),
                    (batch, entity) => batch.Insert(entity)).ConfigureAwait(false);

                await SetHighestSequenceId(persistenceId, highSequenceId).ConfigureAwait(false);
            });

            return await Task<IImmutableList<Exception>>.Factory.ContinueWhenAll(
                groupedTasks.Values.ToArray(),
                tasks => messagesList.Select(
                    m =>
                    {
                        var task = groupedTasks[m.PersistenceId];
                        return task.IsFaulted ? TryUnwrapException(task.Exception) : null;
                    }).ToImmutableList()).ConfigureAwait(false);
        }

        private async Task SetHighestSequenceId(string persistenceId, long highSequenceId)
        {
            var table = _client.Value.GetTableReference(_settings.MetadataTableName);

            var tableResult =
                await table.ExecuteAsync(TableOperation.Retrieve<MetadataEntry>(persistenceId, persistenceId))
                    .ConfigureAwait(false);

            var metadataEntry = tableResult.Result;

            switch (metadataEntry)
            {
                case MetadataEntry me:
                    me.HighestSequenceNr = highSequenceId;
                    await table.ExecuteAsync(TableOperation.Replace(me)).ConfigureAwait(false);
                    break;
                default:
                {
                    await table.ExecuteAsync(TableOperation.Insert(new MetadataEntry(persistenceId, highSequenceId)))
                        .ConfigureAwait(false);
                        break;
                }
            }
        }

        private static TableQuery<JournalEntry> BuildReplayTableQuery(string persistenceId, long fromSequenceNr,
            long toSequenceNr)
        {
            return new TableQuery<JournalEntry>().Where(
                TableQuery.CombineFilters(TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, persistenceId),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual,
                            JournalEntry.ToRowKey(fromSequenceNr))),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual,
                        JournalEntry.ToRowKey(toSequenceNr))));
        }

        private static TableQuery<JournalEntry> BuildDeleteTableQuery(string persistenceId, long sequenceNr)
        {
            return new TableQuery<JournalEntry>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, persistenceId),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual,
                        JournalEntry.ToRowKey(sequenceNr))));
        }

        private IPersistentRepresentation Deserialize(byte[] bytes)
        {
            return (IPersistentRepresentation) _serializer.FromBinary(bytes, PersistentRepresentationType);
        }

        private byte[] Serialize(IPersistentRepresentation message)
        {
            return _serializer.ToBinary(message);
        }

        private JournalEntry ToJournalEntry(IPersistentRepresentation message)
        {
            var payload = Serialize(message);
            return new JournalEntry(message.PersistenceId, message.SequenceNr, payload, message.Manifest);
        }

        private IPersistentRepresentation ToPersistenceRepresentation(JournalEntry entry)
        {
            return Deserialize(entry.Payload);
        }
    }
}