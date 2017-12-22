using System;
using System.Threading;
using RogerWaters.RealTimeDb.EventArgs;

namespace RogerWaters.RealTimeDb.SqlObjects
{
    /// <summary>
    /// Query that encapsulate an Sql-Statement
    /// </summary>
    internal sealed class CustomQuery : SchemaObject, IDisposable
    {
        /// <summary>
        /// Unique identifier for query
        /// </summary>
        private readonly Guid _guid = Guid.NewGuid();

        /// <summary>
        /// Database this query belongs to
        /// </summary>
        private readonly Database _db;

        /// <summary>
        /// UserQuery that is encapsulated by this query
        /// </summary>
        private readonly UserQuery _query;

        /// <summary>
        /// The view that represents the query
        /// </summary>
        private readonly View _view;

        /// <summary>
        /// The name of the view
        /// </summary>
        private readonly SqlObjectName _queryViewName;

        /// <summary>
        /// The lock to ensure that initialization and changes occure in order
        /// </summary>
        private readonly object _dataAccessLock = new object();

        /// <summary>
        /// Create a new instance of <see cref="CustomQuery"/>
        /// </summary>
        /// <param name="db">Database this query belongs to</param>
        /// <param name="query">UserQuery that is encapsulated by this query</param>
        public CustomQuery(Database db, UserQuery query)
        {
            _db = db;
            _query = query;
            _queryViewName = string.Format(db.Config.QueryViewNameTemplate,_guid.ToString().Replace('-','_'));

            db.Config.DatabaseConnectionString.WithConnection
            (
                con =>
                {
                    using (var command = con.CreateCommand())
                    {
                        command.CommandText = $"CREATE VIEW {_queryViewName} AS {Environment.NewLine}{_query.CommandText}";
                        command.ExecuteNonQuery();
                    }
                }
            );
            _view = db.GetOrAddView(_queryViewName,query.PrimaryKeyColumn,query.AdditionalPrimaryKeyColumns);
            lock (_dataAccessLock)
            {
                _view.OnTableDataChanged += DataChanged;
                
                db.Config.DatabaseConnectionString.WithReader($"SELECT * FROM {_queryViewName}", reader =>
                {
                    query.Initialize(reader, _view.CacheTable.Schema);
                });
            }
        }

        /// <summary>
        /// Delegates view changes to <see cref="UserQuery"/>
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
                        _query.RowsInserted(e.Rows);
                        break;
                    case RowChangeKind.UPDATED:
                        _query.RowsUpdated(e.Rows);
                        break;
                    case RowChangeKind.DELETED:
                        _query.RowsDeleted(e.Rows);
                        break;
                }
            }
        }
        
        /// <inheritdoc />
        public void Dispose()
        {
            _view.OnTableDataChanged -= DataChanged;
            CleanupSchemaChanges();
        }
        
        /// <inheritdoc />
        public override void CleanupSchemaChanges()
        {
            _db.RemoveView(_view);
            _view.Dispose();
            _view.CleanupSchemaChanges();

            _db.Config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var command = con.CreateCommand())
                {
                    command.CommandText = $"DROP VIEW {_queryViewName}";
                    command.ExecuteNonQuery();
                }
            });
        }
    }
}
