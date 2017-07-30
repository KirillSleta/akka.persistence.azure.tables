using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.Azure.Tables
{
    public static class BatchOperation
    {
        private const int MaxEntities = 100;

        public static async Task Execute(this CloudTable table, IEnumerable<ITableEntity> entities,
            Action<TableBatchOperation, ITableEntity> operation, long max = 0)
        {
            long count = 0;
            if (entities == null) return;
            var tableEntities = entities as IList<ITableEntity> ?? entities.ToList();
            if (tableEntities.Any())
            {
                var batchOperation = new TableBatchOperation();
                foreach (var s in tableEntities)
                {
                    operation(batchOperation, s);
                    count++;
                    if (count % MaxEntities == 0)
                    {
                        await table.ExecuteBatchAsync(batchOperation);
                        batchOperation = new TableBatchOperation();
                    }
                    if (count == max)
                        return;
                }
                await table.ExecuteBatchAsync(batchOperation);
            }
        }

        public static async Task QuerySegmentedWithCallback<T>(this CloudTable table, TableQuery<T> query,
            Action<T> resultActionCallback, long max = 0) where T : ITableEntity, new()
        {
            long count = 0;
            TableContinuationToken continuationToken = null;
            do
            {
                var tableQueryResult = await table.ExecuteQuerySegmentedAsync(query, continuationToken)
                    .ConfigureAwait(false);
                continuationToken = tableQueryResult.ContinuationToken;
                foreach (var entity in tableQueryResult.Results)
                {
                    resultActionCallback(entity);
                    count++;
                    if (count == max) return;
                }
            } while (continuationToken != null);
        }

        public static async Task QuerySegmentedWithCallback<T>(this CloudTable table, TableQuery<T> query,
            Action<IEnumerable<T>> resultActionCallback) where T : ITableEntity, new()
        {
            TableContinuationToken continuationToken = null;
            do
            {
                var tableQueryResult = await table.ExecuteQuerySegmentedAsync(query, continuationToken);
                continuationToken = tableQueryResult.ContinuationToken;

                resultActionCallback(tableQueryResult.Results);
            } while (continuationToken != null);
        }

        public static async Task<List<T>> QuerySegmented<T>(this CloudTable table, TableQuery<T> query, long max = 0)
            where T : ITableEntity, new()
        {
            var results = new List<T>();
            await QuerySegmentedWithCallback(table, query, element => results.Add(element), max).ConfigureAwait(false);
            return results;
        }
    }
}