using Gerakul.FastSql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gerakul.SqlUtils
{
    public class SqlMetadata
    {
        public string ConnectionString { get; private set; }

        public SqlMetadata(string connectionString)
        {
            this.ConnectionString = connectionString;
        }

        public TableDescription GetTableDescription(string schema, string tableName)
        {
            var columns = MappedCommand.Prepare(ConnectionString, @"
                    select c.column_id as ID, c.name, tp.name as TypeName, 
                        c.max_length as [MaxLength], c.[precision], c.scale, c.is_nullable as IsNullable,
                        ic.key_ordinal as KeyOrdinal
                    from sys.columns c
	                    join sys.tables t on c.object_id = t.object_id
		                    and t.name = @tableName
	                    join sys.schemas s on t.schema_id = s.schema_id
		                    and s.name = @schema
	                    join sys.types tp on c.user_type_id = tp.user_type_id
	                    join sys.indexes i on t.object_id = i.object_id
		                    and i.is_primary_key = 1
	                    left join sys.index_columns ic on i.object_id = ic.object_id and i.index_id = ic.index_id and c.column_id = ic.column_id
                    order by c.column_id", new { schema, tableName })
                .ExecuteQuery<ColumnDescription>(new ExecutionOptions(fieldsSelector: FieldsSelector.Source)).ToList();

            return new TableDescription() { Schema = schema, Name = tableName, Columns = columns };
        }

        public TableDescription GetViewDescription(string schema, string viewName, string key)
        {
            var columns = MappedCommand.Prepare(ConnectionString, @"
                    select c.column_id as ID, c.name, tp.name as TypeName, 
                        c.max_length as [MaxLength], c.[precision], c.scale, c.is_nullable as IsNullable,
                        null as KeyOrdinal
                    from sys.columns c
	                    join sys.views t on c.object_id = t.object_id
		                    and t.name = @viewName
	                    join sys.schemas s on t.schema_id = s.schema_id
		                    and s.name = @schema
	                    join sys.types tp on c.user_type_id = tp.user_type_id
                    order by c.column_id", new { schema, viewName })
                .ExecuteQuery<ColumnDescription>(new ExecutionOptions(fieldsSelector: FieldsSelector.Source)).ToList();

            columns.First(x => x.Name == key).KeyOrdinal = 1;

            return new TableDescription() { Schema = schema, Name = viewName, Columns = columns };
        }
    }
}
