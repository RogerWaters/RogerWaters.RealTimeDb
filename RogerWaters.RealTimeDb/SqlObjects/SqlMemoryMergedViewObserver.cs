using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using RogerWaters.RealTimeDb.Configuration;
using RogerWaters.RealTimeDb.EventArgs;

namespace RogerWaters.RealTimeDb.SqlObjects
{
    //TODO: Merge with insert update delete in seperate resultsets
    //TODO: Query changes direct and do not use Trigger
    //TODO: Change SqlMergedViewObserver to not use trigger probably
    //TODO: - Ether use merge output (will be more efficient)
    //TODO: - Or use same insert update delete queries from here
    //TODO: Allow TableObserver to support memory optimized table... 
    //TODO: - Probably insert changes into Dummy table and use insteadof trigger to redirect messages to queue
    //TODO: After this changes probably it's completely unimportent what changes in table and we can simply observe if any change occurs?
    internal class SqlMemoryMergedViewObserver : IDisposable
    {
        public SqlObjectName ViewName { get; }
        public SqlObjectName CacheTableName { get; }

        internal IReference<TableObserver> CacheTable { get; }
        private readonly Database _db;
        private readonly string[] _keyColumns;

        public event EventHandler<TableDataChangedEventArgs> OnTableDataChanged;

        private readonly AutoResetEvent _mergeEvent = new AutoResetEvent(false);

        private readonly LockedDisposeHelper _disposeHelper = new LockedDisposeHelper();

        public SqlMemoryMergedViewObserver(Database db, SqlObjectName viewName, string primaryKeyColumn, params string[] primaryKeyColumns)
        {
            primaryKeyColumns = primaryKeyColumns ?? new string[0];
            primaryKeyColumns = new[] {primaryKeyColumn}.Union(primaryKeyColumns).ToArray();
            _db = db;
            _keyColumns = primaryKeyColumns;
            
            ViewName = viewName;
            CacheTableName = string.Format(_db.Config.ViewCacheTableNameTemplate, viewName.Schema,viewName.Name);

            var dependencies = LoadDependencies(db.Config, viewName).Select(db.GetOrAddTable).ToList();
            
            db.Config.DatabaseConnectionString.CreateMemoryViewCache(viewName, CacheTableName,primaryKeyColumns);

            var mergeThread = SetupDependencyEvent(dependencies);

            CacheTable = SetupCacheTable(db, CacheTableName, primaryKeyColumns);
            _disposeHelper.Attach(() =>
            {
                mergeThread.Abort();
                mergeThread.Join();
            });
            _disposeHelper.Attach(CacheTable);
            _disposeHelper.Attach(() =>
            {
                _db.Config.DatabaseConnectionString.WithConnection(con =>
                {
                    using (var command = con.CreateCommand())
                    {
                        command.CommandText = $"DROP TABLE {CacheTableName}";
                        command.ExecuteNonQuery();
                    }
                });
            });

            mergeThread.Start();
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

        private Thread SetupDependencyEvent(List<IReference<TableObserver>> dependingTables)
        {
            foreach (var dependency in dependingTables)
            {
                dependency.Value.OnTableDataChanged += OnDependencyChanged;

                _disposeHelper.Attach(() => dependency.Value.OnTableDataChanged -= OnDependencyChanged);
            }

            return new Thread(RefreshViewCache) { IsBackground = true };
        }

        private void OnDependencyChanged(object sender, TableDataChangedEventArgs tableDataChangedEventArgs)
        {
            _mergeEvent.Set();
        }

        private IReference<TableObserver> SetupCacheTable(Database db, string cacheTableName,
            string[] primaryKeyColumns)
        {
            var table = db.GetOrAddTable(cacheTableName,primaryKeyColumns,true);
            table.Value.OnTableDataChanged += _table_OnTableDataChanged;
            return table;
        }


        private void _table_OnTableDataChanged(object sender, TableDataChangedEventArgs e)
        {
            OnTableDataChanged?.Invoke(this,e);
        }

        private void RefreshViewCache()
        {
            List<string> valueColumns = new List<string>();
            foreach (var columnName in CacheTable.Value.Schema.ColumnNamesLookup.Keys)
            {
                if (_keyColumns.Contains(columnName) == false)
                {
                    valueColumns.Add(columnName);
                }
            }

            while (_mergeEvent.WaitOne())
            {
                _db.Config.DatabaseConnectionString.MergeViewChanges(CacheTableName,ViewName,_keyColumns,valueColumns.ToArray());
            }
            // ReSharper disable once FunctionNeverReturns
        }

        public void Dispose()
        {
            _disposeHelper.Dispose();
        }
    }
}