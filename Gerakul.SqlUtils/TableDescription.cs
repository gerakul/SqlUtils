using System;
using System.Collections.Generic;
using System.Text;

namespace Gerakul.SqlUtils
{
    public class TableDescription
    {
        public string Schema;
        public string Name;

        public List<ColumnDescription> Columns;
    }
}
