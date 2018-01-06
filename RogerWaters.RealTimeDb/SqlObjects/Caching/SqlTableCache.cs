using System.Collections.Generic;
using System.Linq;
using RogerWaters.RealTimeDb.Configuration;
using RogerWaters.RealTimeDb.EventArgs;
using RogerWaters.RealTimeDb.Scripts.Queries;

namespace RogerWaters.RealTimeDb.SqlObjects.Caching
{
    internal class SqlTableCache : Cache
    {
        private readonly SqlObjectName _cacheTableName;
        private MergeSelectViewChanges _mergeSelectViewChanges;

        public SqlTableCache(Database db, string query, string[] primaryKeyColumns) : base(db, query, primaryKeyColumns)
        {
            _cacheTableName = string.Format(db.Config.ViewCacheTableNameTemplate, View.ViewName.Schema,View.ViewName.Name);
        }
        
        public override IReadOnlyCollection<Row> Initialize()
        {
            var connectionString = Db.Config.DatabaseConnectionString;

            Schema = new RowSchema(connectionString, View.ViewName, PrimaryKeyColumns);
            
            CreateCacheTable(connectionString,View.ViewName, _cacheTableName, PrimaryKeyColumns);
            //Db.Config.DatabaseConnectionString.CreateMemoryViewCache(_view.ViewName,_cacheTableName,PrimaryKeyColumns);
            DisposeHelper.Attach(() => connectionString.WithConnection(con => con.ExecuteNonQuery($"DROP TABLE {_cacheTableName}")));

            _mergeSelectViewChanges = new MergeSelectViewChanges(_cacheTableName,View.ViewName,Schema.ColumnNames.Values.ToArray(),PrimaryKeyColumns,PrimaryKeyColumns);

            List<Row> rows = new List<Row>();
            connectionString.WithReader($"SELECT * FROM {_cacheTableName}", r =>
            {
                while (r.Read())
                {
                    rows.Add(Schema.ReadRow(r));
                }
            });
            return rows;
        }

        protected virtual void CreateCacheTable(string connectionString, SqlObjectName viewName, SqlObjectName cacheName, string[] primaryColumns)
        {
            connectionString.CreateViewCache(View.ViewName, _cacheTableName, PrimaryKeyColumns);
        }

        public override (IReadOnlyCollection<Row>, IReadOnlyCollection<Row>, IReadOnlyCollection<Row>) CalculateChanges()
        {
            List<Row> insertRows = new List<Row>();
            List<Row> updateRows = new List<Row>();
            List<Row> deleteRows = new List<Row>();
            _mergeSelectViewChanges.Execute(Db.Config.DatabaseConnectionString,
                i =>
                {
                    while (i.Read())
                    {
                        insertRows.Add(Schema.ReadRow(i));
                    }
                },
                u =>
                {
                    while (u.Read())
                    {
                        updateRows.Add(Schema.ReadRow(u));
                    }
                },
                d =>
                {
                    while (d.Read())
                    {
                        deleteRows.Add(Schema.ReadRow(d));
                    }
                });
            return (insertRows, updateRows, deleteRows);
        }
    }
}