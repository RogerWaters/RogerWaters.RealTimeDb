using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using RogerWaters.RealTimeDb.EventArgs;

namespace RogerWaters.RealTimeDb.SqlObjects.Queries
{
    /// <summary>
    /// Query that encapsulate an Sql-Statement
    /// </summary>
    public class MappedSqlCachedQuery<TRow,TKey> : QueryObserver, IEnumerable<TRow>
    {
        /// <summary>
        /// Function to extract <typeparamref name="TRow"/> from <see cref="Row"/>
        /// </summary>
        private readonly Func<Row, TRow> _rowFactory;

        /// <summary>
        /// Function to extract <typeparamref name="TKey"/> from <typeparamref name="TRow"/>
        /// </summary>
        private readonly Func<TRow, TKey> _keyExtractor;
        
        /// <summary>
        /// The lock to ensure that initialization and changes occur in order
        /// </summary>
        private readonly object _dataAccessLock = new object();

        /// <summary>
        /// Cache for rows
        /// </summary>
        private readonly ReducableDictionary<TKey, TRow> _rows;

        private readonly LockedDisposeHelper _disposeHelper = new LockedDisposeHelper();

        /// <summary>
        /// Create a new instance of <see cref="SqlCachedQuery"/>
        /// </summary>
        /// <param name="db">Database this query belongs to</param>
        /// <param name="query">UserQuery that is encapsulated by this query</param>
        /// <param name="keyExtractor">Function to extract <typeparamref name="TKey"/> from <typeparamref name="TRow"/></param>
        /// <param name="keyComparerFactory">Comparer to compare <typeparamref name="TKey"/></param>
        /// <param name="primaryKeyColumn">The column used as primary key</param>
        /// <param name="additionalPrimaryKeyColumns">Additional column names for the primary key</param>
        /// <param name="rowFactory">Function to transform <see cref="Row"/> into <typeparamref name="TRow"/></param>
        internal MappedSqlCachedQuery(Database db, string query, Func<Row,TRow> rowFactory, Func<TRow,TKey> keyExtractor, Func<RowSchema, IEqualityComparer<TKey>> keyComparerFactory, string primaryKeyColumn, params string[] additionalPrimaryKeyColumns) : base(Guid.NewGuid())
        {
            _rowFactory = rowFactory;
            _keyExtractor = keyExtractor;

            var view = SetupView(db, query, primaryKeyColumn, additionalPrimaryKeyColumns);

            lock (_dataAccessLock)
            {
                view.OnTableDataChanged += DataChanged;
                _disposeHelper.Attach(() => view.OnTableDataChanged -= DataChanged);

                RowSchema schema = view.CacheTable.Value.Schema;
                _rows = new ReducableDictionary<TKey, TRow>(keyComparerFactory(schema));

                db.Config.DatabaseConnectionString.WithReader($"SELECT * FROM {view.ViewName}", reader =>
                {
                    while (reader.Read())
                    {
                        var row = schema.ReadRow(reader);
                        var entry = rowFactory(row);
                        var key = keyExtractor(entry);
                        _rows.Add(key, entry);
                    }
                });
                Console.WriteLine(DateTime.Now.TimeOfDay);
            }
        }

        private SqlMemoryMergedViewObserver SetupView(Database db, string query, string primaryKeyColumn, string[] additionalPrimaryKeyColumns)
        {
            var queryViewName = string.Format(db.Config.QueryViewNameTemplate,Guid.ToString().Replace('-','_'));

            db.Config.DatabaseConnectionString.WithConnection
            (
                con =>
                {
                    using (var command = con.CreateCommand())
                    {
                        command.CommandText = $"CREATE VIEW {queryViewName} AS {Environment.NewLine}{query}";
                        command.ExecuteNonQuery();
                    }
                }
            );
            var view = new SqlMemoryMergedViewObserver(db, queryViewName, primaryKeyColumn, additionalPrimaryKeyColumns);
            _disposeHelper.Attach(view);
            _disposeHelper.Attach(() =>
            {
                db.Config.DatabaseConnectionString.WithConnection(con =>
                {
                    using (var command = con.CreateCommand())
                    {
                        command.CommandText = $"DROP VIEW {view.ViewName}";
                        command.ExecuteNonQuery();
                    }
                });
            });
            return view;
        }

        /// <summary>
        /// Process view changes
        /// </summary>
        /// <param name="sender">The SqlObject that created the event</param>
        /// <param name="e">The arguments that are redirected</param>
        private void DataChanged(object sender, TableDataChangedEventArgs e)
        {
            lock (_dataAccessLock)
            {
                switch (e.ChangeKind)
                {
                    case RowChangeKind.INSERTED:
                    case RowChangeKind.UPDATED:
                        foreach (var row in e.Rows)
                        {
                            var entry = _rowFactory(row);
                            var key = _keyExtractor(entry);
                            _rows[key] = entry;
                        }
                        break;
                    case RowChangeKind.DELETED:
                        _rows.RemoveAll(e.Rows.Select(_rowFactory).Select(_keyExtractor));
                        break;
                }
            }
        }
        /// <inheritdoc />
        public override void Dispose()
        {
            _disposeHelper.Dispose();
        }

        /// <inheritdoc />
        public IEnumerator<TRow> GetEnumerator()
        {
            lock (_dataAccessLock)
            {
                return _rows.Values.ToList().GetEnumerator();
            }
        }

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
