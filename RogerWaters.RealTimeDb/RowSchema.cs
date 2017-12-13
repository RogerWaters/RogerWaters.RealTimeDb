using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace RogerWaters.RealTimeDb
{
    public sealed class RowSchema
    {
        private readonly string _connectionString;
        private readonly string _tableName;
        private (Dictionary<int, string>, Dictionary<string, int>, Dictionary<int, Type>) _columns;
        public Dictionary<int, string> ColumnNames => _columns.Item1;
        public Dictionary<string, int> ColumnNamesLookup => _columns.Item2;
        public Dictionary<int, Type> ColumnTypes => _columns.Item3;
        public int ColumnCount => ColumnNames.Count;

        public RowSchema(string connectionString, string tableName)
        {
            _connectionString = connectionString;
            _tableName = tableName;
            _columns = GetTableColumns(connectionString,tableName);
        }

        private (Dictionary<int, string>, Dictionary<string, int>, Dictionary<int, Type>) GetTableColumns(string connectionString, string objectName)
        {
            string query = $"SELECT TOP(0) * FROM [{objectName}]";
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

        public bool TryGetEventRows(XElement root, out IEnumerable<Row> rows, out string eventType)
        {
            eventType = root.Name.LocalName;
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
    }
}
