using System;
using System.Collections.Generic;
using System.Linq;
using RogerWaters.RealTimeDb.Configuration;
using RogerWaters.RealTimeDb.EventArgs;

namespace RogerWaters.RealTimeDb.SqlObjects.Caching
{
    /// <summary>
    /// Caches data
    /// </summary>
    internal abstract class Cache:IDisposable
    {
        /// <summary>
        /// The database the cache belongs to
        /// </summary>
        protected Database Db { get; }
        protected readonly QueryView View;

        protected string Query { get; }
        protected string[] PrimaryKeyColumns { get; }
        
        public event Action Invalidated;
        public RowSchema Schema;

        private readonly CacheWorker.ISignal _handle;
        private static readonly CacheWorker _worker = new CacheWorker();
        protected readonly LockedDisposeHelper DisposeHelper = new LockedDisposeHelper();

        protected virtual void OnInvalidated()
        {
            Invalidated?.Invoke();
        }

        /// <summary>
        /// Create a new cache
        /// </summary>
        /// <param name="db">The database the view belongs to</param>
        /// <param name="schema">The schame that represents the rows</param>
        protected Cache(Database db, string query, string[] primaryKeyColumns)
        {
            Db = db;
            Query = query;
            PrimaryKeyColumns = primaryKeyColumns;
            View = new QueryView(db, query, primaryKeyColumns);
            _handle = _worker.AddWorkerHandle(false,OnInvalidated);

            DisposeHelper.Attach(() => _worker.RemoveWorkerHandle(_handle));
            DisposeHelper.Attach(View);
            
            SetupDependencyEvent
            (
                LoadDependencies(Db.Config, View.ViewName).Select(t =>
                {
                    var reference = Db.GetOrAddTable(t);
                    DisposeHelper.Attach(reference);
                    return reference;
                })
            );
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
                        else if(result.Contains(objectName) == false)
                        {
                            result.Add(objectName);
                        }
                    }
                });
            return result;
        }

        private void SetupDependencyEvent(IEnumerable<IReference<TableObserver>> dependingTables)
        {
            foreach (var dependency in dependingTables)
            {
                dependency.Value.OnTableDataChanged += OnDependencyChanged;

                DisposeHelper.Attach(() => dependency.Value.OnTableDataChanged -= OnDependencyChanged);
            }
        }

        protected void OnDependencyChanged(object sender, TableDataChangedEventArgs e)
        {
            _handle.Set();
        }

        /// <summary>
        /// Setup all dependencies and return the initial state of the cache
        /// </summary>
        public abstract IReadOnlyCollection<Row> Initialize();

        /// <summary>
        /// Caclculate changes 
        /// </summary>
        /// <returns>inserted, updated, deleted rows. Each can be null or empty</returns>
        public abstract (IReadOnlyCollection<Row>,IReadOnlyCollection<Row>,IReadOnlyCollection<Row>) CalculateChanges();

        /// <summary>
        /// Free cache and remove all created objects
        /// </summary>
        public void Dispose()
        {
            DisposeHelper.Dispose();
        }

    }
}
