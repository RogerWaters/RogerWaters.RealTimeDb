using System;
using System.Xml.Linq;
using RogerWaters.RealTimeDb.EventArgs;

namespace RogerWaters.RealTimeDb.SqlObjects
{
    internal sealed class Table : SchemaObject
    {
        private readonly Database _db;
        internal RowSchema Schema { get; }
        public SqlObjectName SqlObjectName { get; }
        public SqlObjectName UpdateTriggerName { get; }
        public SqlObjectName DeleteTriggerName { get; }
        public SqlObjectName InsertTriggerName { get; }
        internal bool Hidden { get; set; }

        public event EventHandler<TableDataChangedEventArgs> OnTableDataChanged;

        public Table(Database db, SqlObjectName sqlObjectName)
        {
            _db = db;
            SqlObjectName = sqlObjectName;
            Schema = new RowSchema(db.Config.DatabaseConnectionString, sqlObjectName);

            InsertTriggerName = string.Format(db.Config.TriggerNameTemplate, sqlObjectName.Schema, "insert", sqlObjectName.Name);
            DeleteTriggerName = string.Format(db.Config.TriggerNameTemplate, sqlObjectName.Schema, "delete", sqlObjectName.Name);
            UpdateTriggerName = string.Format(db.Config.TriggerNameTemplate, sqlObjectName.Schema, "update", sqlObjectName.Name);

            SetupDatabaseSchema(sqlObjectName, db);
        }

        private void SetupDatabaseSchema(SqlObjectName sqlObjectName, Database db)
        {
            db.Config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var transaction = con.BeginTransaction())
                {
                    transaction.CreateTriggerDelete(DeleteTriggerName, sqlObjectName, db.Conversation.ToString(),
                        db.MessageTypeName);
                    transaction.CreateTriggerInsert(InsertTriggerName, sqlObjectName, db.Conversation.ToString(),
                        db.MessageTypeName);
                    transaction.CreateTriggerUpdate(UpdateTriggerName, sqlObjectName, db.Conversation.ToString(),
                        db.MessageTypeName);
                    transaction.Commit();
                }
            });
        }

        internal void OnReceive(XElement result)
        {
            if (Schema.TryGetEventRows(result, out var rows, out var type))
            {
                OnOnTableDataChanged(new TableDataChangedEventArgs(rows, type));
            }
        }

        private void OnOnTableDataChanged(TableDataChangedEventArgs e)
        {
            //Console.WriteLine("{0} on {1} with {2} rows", e.ChangeKind, SqlObjectName, e.Rows.Count);
            OnTableDataChanged?.Invoke(this, e);
        }

        public override void CleanupSchemaChanges()
        {
            var config = _db.Config;
            config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var transaction = con.BeginTransaction())
                {
                    transaction.DropTrigger(DeleteTriggerName);
                    transaction.DropTrigger(InsertTriggerName);
                    transaction.DropTrigger(UpdateTriggerName);

                    transaction.Commit();
                }
            });
        }
    }
}