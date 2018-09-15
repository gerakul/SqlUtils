using System.Collections.Generic;

namespace Gerakul.SqlUtils
{
    public class TableDescription
    {
        public string Schema;
        public string Name;

        public List<ColumnDescription> Columns;
    }
}
