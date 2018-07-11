using System;
using System.Collections.Generic;
using System.Text;

namespace Gerakul.SqlUtils
{
    public class ColumnDescription
    {
        public int ID;
        public string Name;
        public string TypeName;
        public short MaxLength;
        public byte Precision;
        public byte Scale;
        public bool IsNullable;
        public byte? KeyOrdinal;

        public bool IsKey
        {
            get
            {
                return KeyOrdinal.HasValue;
            }
        }

        public string GetTypeDeclaration(bool forHistory = false)
        {
            switch (TypeName.ToLowerInvariant())
            {
                case "time":
                case "datetime2":
                case "datetimeoffset":
                    return $"[{TypeName}]({Scale})";
                case "decimal":
                case "numeric":
                    return $"[{TypeName}]({Precision}, {Scale})";
                case "binary":
                case "char":
                    return $"[{TypeName}]({MaxLength})";
                case "nchar":
                    return $"[{TypeName}]({MaxLength / 2})";
                case "nvarchar":
                    return MaxLength == -1 ? $"[{TypeName}](max)" : $"[{TypeName}]({MaxLength / 2})";
                case "varbinary":
                case "varchar":
                    return MaxLength == -1 ? $"[{TypeName}](max)" : $"[{TypeName}]({MaxLength})";
                case "xml":
                    throw new NotSupportedException("data type xml");
                case "timestamp":
                    return forHistory ? "[binary](8)" : $"[{TypeName}]";
                default:
                    return $"[{TypeName}]";
            }
        }

        public string GetColumnDeclaration(bool forHistory = false)
        {
            return $"[{Name}] {GetTypeDeclaration(forHistory)} {(forHistory || IsNullable ? "NULL" : "NOT NULL")}";
        }
    }
}
