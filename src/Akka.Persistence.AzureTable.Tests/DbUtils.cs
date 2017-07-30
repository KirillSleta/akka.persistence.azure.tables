using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Akka.Persistence.Azure.Tables.Tests
{
    public static class DbUtils
    {
        public static async Task CleanAsync(string connectionString, string tableName)
        {
            var tableClient = CloudStorageAccount.Parse(connectionString).CreateCloudTableClient();
            var table = tableClient.GetTableReference(tableName);
            await table.CreateIfNotExistsAsync();
            var query = new TableQuery<DynamicTableEntity>();
            await table.QuerySegmentedWithCallback(query,
                async results => await table.Execute(results, (batch, entity) => batch.Delete(entity)));
        }
    }
}