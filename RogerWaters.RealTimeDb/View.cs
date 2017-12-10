using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using RogerWaters.RealTimeDb.EventArgs;

namespace RogerWaters.RealTimeDb
{
    public sealed class View : SchemaObject, IDisposable
    {
        public string ViewName { get; }
        internal bool Hidden { get; set; }

        internal List<Table> Dependencies { get; }
        public string CacheTableName { get; }
        private readonly Table _table;
        private readonly IDisposable _subsciption;
        private readonly Database _db;
        private readonly string[] _keyColumns;

        public event EventHandler<TableDataChangedEventArgs> OnTableDataChanged;

        public View(Database db, string viewName, string primaryKeyColumn, params string[] primaryKeyColumns)
        {
            primaryKeyColumns = primaryKeyColumns ?? new string[0];
            primaryKeyColumns = new[] {primaryKeyColumn}.Union(primaryKeyColumns).ToArray();
            _db = db;
            _keyColumns = primaryKeyColumns;
            
            ViewName = viewName;
            CacheTableName = string.Format(_db.Config.ViewCacheTableNameTemplate, viewName);

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

            _subsciption = SetupDependencyEvent(Dependencies, new List<View>());
            _table = SetupCacheTable(db, CacheTableName);
        }

        private List<string> LoadDependencies(DatabaseConfig config, string viewName)
        {
            List<string> result = new List<string>();
            config.DatabaseConnectionString.WithReader(
                $@"SELECT DISTINCT referenced_entity_name, obj.type
            FROM
                sys.dm_sql_referenced_entities('dbo.{viewName}', 'OBJECT') e
            LEFT JOIN
                sys.objects obj
            ON
                obj.object_id = referenced_id",
                reader =>
                {
                    while (reader.Read())
                    {
                        if (((string) reader[1]).Trim() == "V")
                        {
                            result.AddRange(LoadDependencies(config,reader.GetString(0)));
                        }
                        else
                        {
                            result.Add(reader.GetString(0));
                        }
                    }
                });
            return result;
        }

        private IDisposable SetupDependencyEvent(List<Table> dependingTables, List<View> dependingViews)
        {
            var dataEvents = Observable.FromEventPattern<TableDataChangedEventArgs>(
                h =>
                {
                    foreach (var dependency in dependingTables)
                    {
                        dependency.OnTableDataChanged += h;
                    }
                    foreach (var dependency in dependingViews)
                    {
                        dependency.OnTableDataChanged += h;
                    }
                },
                h =>
                {
                    foreach (var dependency in dependingTables)
                    {
                        dependency.OnTableDataChanged -= h;
                    }
                    foreach (var dependency in dependingViews)
                    {
                        dependency.OnTableDataChanged -= h;
                    }
                });
            return dataEvents.Throttle(TimeSpan.FromMilliseconds(100))
                .Subscribe(pattern => RefreshViewCache());
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
            foreach (var columnName in _table.Schema.ColumnNamesLookup.Keys)
            {
                if (_keyColumns.Contains(columnName) == false)
                {
                    valueColumns.Add(columnName);
                }
            }
            _db.Config.DatabaseConnectionString.MergeViewChanges(CacheTableName,ViewName,_keyColumns,valueColumns.ToArray());
        }

        public void Dispose()
        {
            _subsciption?.Dispose();
        }

        public override void CleanupSchemaChanges()
        {
            Dispose();
            _db.RemoveTable(_table);
            _table.CleanupSchemaChanges();
            _db.Config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var command = con.CreateCommand())
                {
                    command.CommandText = $"DROP TABLE [{CacheTableName}]";
                    command.ExecuteNonQuery();
                }
            });
        }
    }
}
