using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Xml.Linq;
using RogerWaters.RealTimeDb.SqlObjects;

namespace RogerWaters.RealTimeDb
{
    public sealed class RowSchema
    {
        private readonly string _connectionString;
        private readonly SqlObjectName _tableName;
        public Dictionary<string,int> PrimaryKeyColumns { get; }
        private (Dictionary<int, string>, Dictionary<string, int>, Dictionary<int, Type>) _columns;
        public Dictionary<int, string> ColumnNames => _columns.Item1;
        public Dictionary<string, int> ColumnNamesLookup => _columns.Item2;
        public Dictionary<int, Type> ColumnTypes => _columns.Item3;
        public int ColumnCount => ColumnNames.Count;
        public IEqualityComparer<Row> RowEqualityComparer { get; }
        public IEqualityComparer<object[]> RowKeyEqualityComparer { get; }

        public RowSchema(string connectionString, SqlObjectName tableName)
        {
            _connectionString = connectionString;
            _tableName = tableName;
            _columns = GetTableColumns(connectionString,tableName);
            PrimaryKeyColumns = connectionString.GetPrimaryKeyColumns(tableName).ToDictionary(c => c, c => ColumnNamesLookup[c]);
            RowEqualityComparer = new RowComparer();
            RowKeyEqualityComparer = new RowKeyComparer();
        }
        
        private (Dictionary<int, string>, Dictionary<string, int>, Dictionary<int, Type>) GetTableColumns(string connectionString, SqlObjectName objectName)
        {
            string query = $"SELECT TOP(0) * FROM {objectName}";
            var names = new Dictionary<int, string>();
            var namesLookup = new Dictionary<string, int>();
            var types = new Dictionary<int, Type>();
            connectionString.WithReader(query, reader =>
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    names.Add(i,reader.GetName(i));
                    namesLookup.Add(reader.GetName(i),i);
                    types.Add(i,reader.GetFieldType(i));
                }
            });
            return (names, namesLookup, types);
        }

        public bool TryGetEventRows(XElement root, out IReadOnlyList<Row> rows, out RowChangeKind eventType)
        {
            if (Enum.TryParse(root.Name.LocalName, out eventType) == false)
            {
                rows = new List<Row>(0);
                return false;
            }

            List<Row> rowsReturn = new List<Row>();
            foreach (var rowElement in root.Descendants("row"))
            {
                var row = new Row(this);
                foreach (var columnElement in rowElement.Elements())
                {
                    int id = ColumnNamesLookup[columnElement.Name.LocalName];
                    if (columnElement.Attribute(XName.Get("nil", "http://www.w3.org/2001/XMLSchema-instance")) != null)
                    {
                        row[id] = null;
                    }
                    else
                    {
                        row[id] = Convert.ChangeType(columnElement.Value, ColumnTypes[id]);
                    }
                }
                rowsReturn.Add(row);
            }
            rows = rowsReturn;
            return true;
        }

        public Row ReadRow(SqlDataReader reader)
        {
            var row = new Row(this);
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[i] = reader[i];
            }
            return row;
        }

        public object[] GetKey(Row row)
        {
            return PrimaryKeyColumns.Values.Select(i => row[i]).ToArray();
        }

        private class RowComparer : IEqualityComparer<Row>
        {
            public bool Equals(Row x, Row y)
            {
                if (ReferenceEquals(x,y) == false)
                {
                    if (ReferenceEquals(x, null) || ReferenceEquals(y, null))
                    {
                        return false;
                    }
                    if (x.Count != y.Count)
                    {
                        return false;
                    }
                    for (int i = 0; i < x.Count; i++)
                    {
                        if (x[i].Equals(y[i]) == false)
                        {
                            return false;
                        }
                    }
                }
                return true;
            }

            public int GetHashCode(Row obj)
            {
                var hashCode = 375;
                hashCode ^= obj.Count * 7;
                foreach (var e in obj)
                {
                    hashCode ^= e.GetHashCode() * 7;
                }

                return hashCode;
            }
        }

        private class RowKeyComparer : IEqualityComparer<object[]>
        {
            public bool Equals(object[] x, object[] y)
            {
                if (ReferenceEquals(x,y) == false)
                {
                    if (ReferenceEquals(x, null) || ReferenceEquals(y, null))
                    {
                        return false;
                    }
                    if (x.Length != y.Length)
                    {
                        return false;
                    }
                    for (int i = 0; i < x.Length; i++)
                    {
                        if (x[i].Equals(y[i]) == false)
                        {
                            return false;
                        }
                    }
                }
                return true;
            }

            public int GetHashCode(object[] obj)
            {
                var hashCode = 375;
                hashCode ^= obj.Length * 7;
                foreach (var o in obj)
                {
                    hashCode ^= o.GetHashCode() * 7;
                }

                return hashCode;
            }
        }
    }
}
