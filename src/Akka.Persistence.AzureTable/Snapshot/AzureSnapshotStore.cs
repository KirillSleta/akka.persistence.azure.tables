using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Persistence.Snapshot;
using Akka.Serialization;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.Azure.Tables.Snapshot
{
    public class AzureSnapshotStore : SnapshotStore
    {
        private static readonly Type SnapshotType = typeof(Serialization.Snapshot);
        private readonly Serializer _serializer;
        private readonly AzureTableSnapshotStoreSettings _settings;
        private CloudTableClient _client;
        private CloudTable _table;

        public AzureSnapshotStore()
        {
            _settings = AzureTablePersistence.Get(Context.System).SnapshotStoreSettings;
            _serializer = Context.System.Serialization.FindSerializerForType(SnapshotType);
        }

        protected override void PreStart()
        {
            base.PreStart();

            _client = CloudStorageAccount.Parse(_settings.ConnectionString).CreateCloudTableClient();

            if (_settings.AutoInitialize)
                _client.GetTableReference(_settings.TableName).CreateIfNotExistsAsync().Wait();

            _table = _client.GetTableReference(_settings.TableName);
        }

        /// <summary>
        ///     Asynchronously loads a snapshot.
        ///     This call is protected with a circuit-breaker
        /// </summary>
        /// <param name="persistenceId">Persistent actor identifier</param>
        protected override Task<SelectedSnapshot> LoadAsync(string persistenceId,
            SnapshotSelectionCriteria criteria)
        {
            var query = BuildSnapshotTableQuery(persistenceId, criteria);
            var snapshots = _table.QuerySegmented(query).Result;
            return Task.FromResult(snapshots.OrderByDescending(t => t.RowKey).Select(ToSelectedSnapshot)
                .FirstOrDefault());
        }

        /// <summary>
        ///     Asynchronously saves a snapshot.
        ///     This call is protected with a circuit-breaker
        /// </summary>
        protected override Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var upsertOperation = TableOperation.Insert(ToSnapshotEntry(metadata, snapshot));

            var entity = _table.ExecuteAsync(TableOperation.Retrieve<SnapshotEntry>(metadata.PersistenceId,
                SnapshotEntry.ToRowKey(metadata.SequenceNr))).Result;

            var snapEntity = (SnapshotEntry) entity.Result;
            if (snapEntity != null)
            {
                snapEntity.Payload = Serialize(snapshot);
                upsertOperation = TableOperation.Replace(snapEntity);
            }

            return _table.ExecuteAsync(upsertOperation);
        }

        /// <summary>
        ///     Deletes the snapshot identified by <paramref name="metadata" />.
        ///     This call is protected with a circuit-breaker
        /// </summary>
        protected override Task DeleteAsync(SnapshotMetadata metadata)
        {
            var getOperation =
                TableOperation.Retrieve<SnapshotEntry>(metadata.PersistenceId,
                    SnapshotEntry.ToRowKey(metadata.SequenceNr));
            var result = _table.ExecuteAsync(getOperation).Result;
            var deleteOperation = TableOperation.Delete((SnapshotEntry) result.Result);
            return _table.ExecuteAsync(deleteOperation);
        }

        /// <summary>
        ///     Deletes all snapshots matching provided <paramref name="criteria" />.
        ///     This call is protected with a circuit-breaker
        /// </summary>
        /// <param name="persistenceId">Persistent actor identifier</param>
        protected override Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var query = BuildSnapshotTableQuery(persistenceId, criteria);

            void Callback(IEnumerable<SnapshotEntry> results)
            {
                _table.Execute(results, (batch, entity) => batch.Delete(entity)).Wait();
            }

            return _table.QuerySegmentedWithCallback(query,
                Callback);
        }

        private object Deserialize(byte[] bytes)
        {
            return ((Serialization.Snapshot) _serializer.FromBinary(bytes, SnapshotType)).Data;
        }

        private byte[] Serialize(object snapshotData)
        {
            return _serializer.ToBinary(new Serialization.Snapshot(snapshotData));
        }


        private static TableQuery<SnapshotEntry> BuildSnapshotTableQuery(string persistenceId,
            SnapshotSelectionCriteria criteria)
        {
            var comparsion = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, persistenceId);

            if (criteria.MaxSequenceNr > 0)
                comparsion = TableQuery.CombineFilters(
                    comparsion,
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual,
                        SnapshotEntry.ToRowKey(criteria.MaxSequenceNr)));

            if (criteria.MinSequenceNr > 0)
                comparsion = TableQuery.CombineFilters(
                    comparsion,
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual,
                        SnapshotEntry.ToRowKey(criteria.MinSequenceNr)));

            if (criteria.MaxTimeStamp.Ticks > 0)
                comparsion = TableQuery.CombineFilters(
                    comparsion,
                    TableOperators.And,
                    TableQuery.GenerateFilterConditionForLong("SnapshotTimestamp", QueryComparisons.LessThanOrEqual,
                        criteria.MaxTimeStamp.Ticks));
            if (criteria.MinTimestamp?.Ticks > 0)
                comparsion = TableQuery.CombineFilters(
                    comparsion,
                    TableOperators.And,
                    TableQuery.GenerateFilterConditionForLong("SnapshotTimestamp", QueryComparisons.GreaterThanOrEqual,
                        criteria.MinTimestamp.Value.Ticks));


            return new TableQuery<SnapshotEntry>().Where(comparsion);
        }

        private SnapshotEntry ToSnapshotEntry(SnapshotMetadata metadata, object snapshot)
        {
            var payload = Serialize(snapshot);
            return new SnapshotEntry(metadata.PersistenceId, metadata.SequenceNr, metadata.Timestamp.Ticks, payload);
        }

        private SelectedSnapshot ToSelectedSnapshot(SnapshotEntry entry)
        {
            var payload = Deserialize(entry.Payload);
            return new SelectedSnapshot(
                new SnapshotMetadata(entry.PartitionKey, long.Parse(entry.RowKey),
                    new DateTime(entry.SnapshotTimestamp)), payload);
        }
    }
}