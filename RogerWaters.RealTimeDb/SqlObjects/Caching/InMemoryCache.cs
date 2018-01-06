using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RogerWaters.RealTimeDb.SqlObjects.Caching
{
    internal class InMemoryCache:Cache
    {
        private ReducableDictionary<object[],Row> _data;
        public InMemoryCache(Database db, string query, string[] primaryKeyColumns) : base(db, query, primaryKeyColumns)
        {
        }

        public override IReadOnlyCollection<Row> Initialize()
        {
            Db.Config.DatabaseConnectionString.WithReader(Query, reader =>
            {
                Schema = new RowSchema(reader, PrimaryKeyColumns);
                _data = new ReducableDictionary<object[], Row>(Schema.RowKeyEqualityComparer);
                while (reader.Read())
                {
                    var row = Schema.ReadRow(reader);
                    _data.Add(row.Key,row);
                }
            });
            return _data.Values.ToArray();
        }

        public override (IReadOnlyCollection<Row>, IReadOnlyCollection<Row>, IReadOnlyCollection<Row>) CalculateChanges()
        {
            List<Row> inserted = new List<Row>();
            List<Row> updated = new List<Row>();
            List<Row> deleted = new List<Row>();

            Db.Config.DatabaseConnectionString.WithReader(Query, reader =>
            {
                Dictionary<object[],Row> oldState = new Dictionary<object[], Row>(_data, Schema.RowKeyEqualityComparer);
                while (reader.Read())
                {
                    var row = Schema.ReadRow(reader);
                    if (_data.TryGetValue(row.Key, out Row oldRow))
                    {
                        oldState.Remove(row.Key);
                        if (Schema.RowEqualityComparer.Equals(row,oldRow) == false)
                        {
                            updated.Add(row);
                            _data[row.Key] = row;
                        }
                    }
                    else
                    {
                        inserted.Add(row);
                        _data.Add(row.Key,row);
                    }
                }

                deleted = oldState.Values.ToList();
                _data.RemoveAll(oldState.Keys);
            });
            return (inserted, updated, deleted);
        }
    }
}
