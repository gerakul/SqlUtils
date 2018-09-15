using Gerakul.FastSql.Common;
using Gerakul.FastSql.SqlServer;
using System;
using System.Linq;

namespace Gerakul.SqlUtils
{
    public class HistoryGenerator
    {
        private ConnectionStringContext context;
        protected SqlMetadata metadata;

        public string ConnectionString { get; private set; }

        public HistoryGenerator(string connectionString)
        {
            this.ConnectionString = connectionString;
            this.metadata = new SqlMetadata(connectionString);
            this.context = SqlContextProvider.DefaultInstance.CreateContext(connectionString);
        }

        public void ExecuteHistoryScript(string originalSchema, string originalTableName,
            string historySchema, string historyTableName)
        {
            ExecuteHistoryScript(metadata.GetTableDescription(originalSchema, originalTableName),
                historySchema, historyTableName);
        }

        public void ExecuteHistoryScript(TableDescription tableDescription,
            string historySchema, string historyTableName)
        {
            string table = GenerateHistoryTable(tableDescription, historySchema, historyTableName);
            string trigger = GenerateHistoryTrigger(tableDescription, historySchema, historyTableName);

            context.UsingTransaction(tc =>
            {
                tc.CreateSimple(table).ExecuteNonQuery();
                tc.CreateSimple(trigger).ExecuteNonQuery();
            });
        }

        public virtual string GenerateHistoryTable(TableDescription tableDescription,
            string historySchema, string historyTableName)
        {
            return $@"
CREATE TABLE [{historySchema}].[{historyTableName}](
	[H_ID] [bigint] IDENTITY(1,1) NOT NULL,
	[H_ConnectionID] [uniqueidentifier] NOT NULL,
	[H_TransactionID] [bigint] NOT NULL,
	[H_SessionID] [int] NOT NULL,
	[H_Login] [nvarchar](128) NOT NULL,
	[H_Time] [datetime2](7) NOT NULL,
	[H_OperationType] [int] NOT NULL,
	[H_IsNew] [bit] NOT NULL,
{string.Join(Environment.NewLine, tableDescription.Columns.Select(x => $"	{x.GetColumnDeclaration(true)},"))}
 CONSTRAINT [PK_{historyTableName}_History] PRIMARY KEY CLUSTERED 
(
	[H_ID] ASC
)
); 
";
        }

        public virtual string GenerateHistoryTrigger(TableDescription tableDescription,
            string historySchema, string historyTableName)
        {
            return $@"
CREATE TRIGGER [{tableDescription.Schema}].[{tableDescription.Name}History]
   ON  [{tableDescription.Schema}].[{tableDescription.Name}]
   AFTER INSERT, UPDATE, DELETE
AS 
BEGIN

	SET NOCOUNT ON;

declare @delExists bit;
declare @insExists bit;

if exists(select top 1 1 from deleted) set @delExists = 1
if exists(select top 1 1 from inserted) set @insExists = 1

declare @opType int;

if (@delExists = 1)
	if (@insExists = 1)
		set @opType = 2
	else 
		set @opType = 3
else
	if (@insExists = 1)
		set @opType = 1
	else 
		set @opType = 0
		
if (@opType = 0)
	return;	

declare @time datetime2(7) = SYSUTCDATETIME()
declare @connection_id uniqueidentifier = (select connection_id from sys.dm_exec_connections where session_id = @@SPID and parent_connection_id is null)
--declare @transaction_id bigint = (select transaction_id from sys.dm_tran_current_transaction)
declare @transaction_id bigint = (select transaction_id from sys.dm_tran_session_transactions where session_id = @@SPID)
declare @login nvarchar(128) = ORIGINAL_LOGIN()


insert into [{historySchema}].[{historyTableName}] ([H_ConnectionID], [H_TransactionID], [H_SessionID], [H_Login], [H_Time], [H_OperationType], [H_IsNew]
-- data columns
{string.Join(Environment.NewLine, tableDescription.Columns.Select(x => $"	,[{x.Name}]"))})
select @connection_id, @transaction_id, @@SPID, @login, @time, @opType, h.H_IsNew
-- data columns
{string.Join(Environment.NewLine, tableDescription.Columns.Select(x => $"	,h.[{x.Name}]"))}
from 
(
	select 0 as H_IsNew, t.* 
	from deleted t
	union all
	select 1 as H_IsNew, t.* 
	from inserted t
) h
order by {string.Join(", ", tableDescription.Columns.Where(x => x.IsKey).OrderBy(x => x.KeyOrdinal).Select(x => $"h.{x.Name}"))}, h.H_IsNew


END;

ALTER TABLE [{tableDescription.Schema}].[{tableDescription.Name}] ENABLE TRIGGER [{tableDescription.Name}History];
";
        }
    }
}
