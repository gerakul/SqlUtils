# SqlUtils
Some extended utils for work with MS SQL Server using FastSql

## Samples:

1. Retrieving table metadata

  ```csharp
  var sqlmetadata = new SqlMetadata("your connection string");
  var t = sqlmetadata.GetTableDescription("schema", "Table");
  ```  

2. Creation of history table and trigger

  ```csharp
  var gen = new HistoryGenerator("your connection string");
  gen.ExecuteHistoryScript("schema", "Table", "historyschema", "HistoryTable");
  ```  