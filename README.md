# Akka.Persistence.Azure.Tables

Akka Persistence journal and snapshot store backed by Azure Table service.

.NET Standart 1.6 package

## Configuration

```hocon
akka.persistence {
    journal {
        azure-table {
            # qualified type name of the Azure Storage Table persistence journal actor
            class = "Akka.Persistence.Azure.Tables.Journal.AzureTableJournal, Akka.Persistence.AzureTable"

            # dispatcher used to drive journal actor
            plugin-dispatcher = "akka.actor.default-dispatcher"

			# connection string used for database access
			connection-string = "UseDevelopmentStorage=true"

			# table storage table corresponding with persistent journal
			table-name = events

			# metadata table
			metadata-table-name = metadata

			# should corresponding journal table be initialized automatically
			auto-initialize = off
        }
    }
    snapshot-store {
        azure-table {
            # qualified type name of the Azure Storage Table persistence snapshot-store actor
            class = "Akka.Persistence.Azure.Tables.Snapshot.AzureSnapshotStore, Akka.Persistence.AzureTable"

            # dispatcher used to drive snapshot-store actor
            plugin-dispatcher = "akka.actor.default-dispatcher"

			# connection string used for database access
			connection-string = "UseDevelopmentStorage=true"

			# table storage table corresponding with persistent snapshot-store
			table-name = snapshots

			# should corresponding snapshot-store table be initialized automatically
			auto-initialize = off
        }
    }    
}
```

## Serialization
Azure plugin uses Akka.Net object serializer to serialize all payloads
