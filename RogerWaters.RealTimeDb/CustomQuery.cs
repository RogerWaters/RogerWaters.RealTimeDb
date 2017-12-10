using System;
using RogerWaters.RealTimeDb.EventArgs;

namespace RogerWaters.RealTimeDb
{
    public sealed class CustomQuery : SchemaObject, IDisposable
    {
        private readonly Guid _guid = Guid.NewGuid();
        private readonly Database _db;
        private readonly UserQuery _query;
        private readonly View _view;
        public string QueryViewName { get; }
        private readonly object _dataAccessLock = new object();

        public CustomQuery(Database db, UserQuery query)
        {
            _db = db;
            _query = query;
            QueryViewName = string.Format(db.Config.QueryViewNameTemplate,_guid.ToString().Replace('-','_'));

            db.Config.DatabaseConnectionString.WithConnection
            (
                con =>
                {
                    using (var command = con.CreateCommand())
                    {
                        command.CommandText = $"CREATE VIEW {QueryViewName} AS {Environment.NewLine}{_query.CommandText}";
                        command.ExecuteNonQuery();
                    }
                }
            );
            _view = db.GetOrAddView(QueryViewName,query.PrimaryKeyColumn,query.AdditionalPrimaryKeyColumns);

            lock (_dataAccessLock)
            {
                _view.OnTableDataChanged += DataChanged;
                db.Config.DatabaseConnectionString.WithReader($"SELECT * FROM [{QueryViewName}]", query.Initialize);
            }
        }

        private void DataChanged(object sender, TableDataChangedEventArgs e)
        {
            lock (_dataAccessLock)
            {
                switch (e.ChangeType)
                {
                    case "INSERTED":
                        _query.RowsInserted(e.Rows);
                        break;
                    case "UPDATED":
                        _query.RowsUpdated(e.Rows);
                        break;
                    case "DELETED":
                        _query.RowsDeleted(e.Rows);
                        break;
                }
            }
        }

        public void Dispose()
        {
            CleanupSchemaChanges();
        }

        public override void CleanupSchemaChanges()
        {
            _db.RemoveView(_view);
            _view.Dispose();
            _view.CleanupSchemaChanges();

            _db.Config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var command = con.CreateCommand())
                {
                    command.CommandText = $"DROP VIEW [{QueryViewName}]";
                    command.ExecuteNonQuery();
                }
            });
        }
    }
}
