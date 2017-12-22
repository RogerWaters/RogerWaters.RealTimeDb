using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using RogerWaters.RealTimeDb.EventArgs;

namespace RogerWaters.RealTimeDb.SqlObjects
{
    internal sealed class View : SchemaObject, IDisposable
    {
        public SqlObjectName ViewName { get; }
        internal bool Hidden { get; set; }

        private List<Table> Dependencies { get; }
        public SqlObjectName CacheTableName { get; }
        internal Table CacheTable { get; }
        private readonly Database _db;
        private readonly string[] _keyColumns;

        public event EventHandler<TableDataChangedEventArgs> OnTableDataChanged;

        private readonly AutoResetEvent _mergeEvent = new AutoResetEvent(false);
        private readonly Thread _mergeThread;

        public View(Database db, SqlObjectName viewName, string primaryKeyColumn, params string[] primaryKeyColumns)
        {
            primaryKeyColumns = primaryKeyColumns ?? new string[0];
            primaryKeyColumns = new[] {primaryKeyColumn}.Union(primaryKeyColumns).ToArray();
            _db = db;
            _keyColumns = primaryKeyColumns;
            
            ViewName = viewName;
            CacheTableName = string.Format(_db.Config.ViewCacheTableNameTemplate, viewName.Schema,viewName.Name);

            Dependencies = LoadDependencies(db.Config, viewName).Select(db.GetOrAddTable).ToList();
            
            db.Config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var transaction = con.BeginTransaction())
                {
                    transaction.CreateViewCache(viewName, CacheTableName);
                    transaction.CreateViewCachePrimaryIndex(CacheTableName, primaryKeyColumns);
                    transaction.Commit();
                }
            });

            _mergeThread = SetupDependencyEvent(Dependencies);

            CacheTable = SetupCacheTable(db, CacheTableName);

            _mergeThread.Start();
        }

        private List<SqlObjectName> LoadDependencies(DatabaseConfig config, SqlObjectName viewName)
        {
            List<SqlObjectName> result = new List<SqlObjectName>();
            config.DatabaseConnectionString.WithReader(
                $@"SELECT DISTINCT referenced_schema_name, referenced_entity_name, obj.type
            FROM
                sys.dm_sql_referenced_entities('{viewName}', 'OBJECT') e
            LEFT JOIN
                sys.objects obj
            ON
                obj.object_id = referenced_id",
                reader =>
                {
                    while (reader.Read())
                    {
                        var objectName = new SqlObjectName(reader[1] as string, reader[0] as string);
                        if (((string) reader[2]).Trim() == "V")
                        {
                            result.AddRange(LoadDependencies(config,objectName));
                        }
                        else
                        {
                            result.Add(objectName);
                        }
                    }
                });
            return result;
        }

        private Thread SetupDependencyEvent(List<Table> dependingTables)
        {
            foreach (var dependency in dependingTables)
            {
                dependency.OnTableDataChanged += OnDependencyChanged;
            }

            return new Thread(RefreshViewCache) { IsBackground = true };
        }

        private void OnDependencyChanged(object sender, TableDataChangedEventArgs tableDataChangedEventArgs)
        {
            _mergeEvent.Set();
        }

        private Table SetupCacheTable(Database db, string cacheTableName)
        {
            var table = db.GetOrAddTable(cacheTableName);
            table.Hidden = true;
            table.OnTableDataChanged += _table_OnTableDataChanged;
            return table;
        }


        private void _table_OnTableDataChanged(object sender, TableDataChangedEventArgs e)
        {
            OnTableDataChanged?.Invoke(this,e);
        }

        private void RefreshViewCache()
        {
            List<string> valueColumns = new List<string>();
            foreach (var columnName in CacheTable.Schema.ColumnNamesLookup.Keys)
            {
                if (_keyColumns.Contains(columnName) == false)
                {
                    valueColumns.Add(columnName);
                }
            }

            while (true)
            {
                _mergeEvent.WaitOne();
                _db.Config.DatabaseConnectionString.MergeViewChanges(CacheTableName,ViewName,_keyColumns,valueColumns.ToArray());
            }
            // ReSharper disable once FunctionNeverReturns
        }

        public void Dispose()
        {
            Dependencies.ForEach(t => t.OnTableDataChanged -= OnDependencyChanged);
            _mergeThread.Abort();
            _mergeThread.Join();
        }

        public override void CleanupSchemaChanges()
        {
            Dispose();
            _db.RemoveTable(CacheTable);
            CacheTable.CleanupSchemaChanges();
            _db.Config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var command = con.CreateCommand())
                {
                    command.CommandText = $"DROP TABLE {CacheTableName}";
                    command.ExecuteNonQuery();
                }
            });
        }
    }
}
