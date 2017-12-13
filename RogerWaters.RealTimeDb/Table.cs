using System;
using System.Collections.Generic;
using System.Xml.Linq;
using RogerWaters.RealTimeDb.EventArgs;

namespace RogerWaters.RealTimeDb
{
    public sealed class Table : SchemaObject
    {
        private readonly Database _db;
        internal RowSchema Schema { get; }
        public string TableName { get; }
        public string UpdateTriggerName { get; }
        public string DeleteTriggerName { get; }
        public string InsertTriggerName { get; }
        internal bool Hidden { get; set; }

        public event EventHandler<TableDataChangedEventArgs> OnTableDataChanged;

        public Table(Database db, string tableName)
        {
            _db = db;
            TableName = tableName;
            Schema = new RowSchema(db.Config.DatabaseConnectionString,tableName);
            
            InsertTriggerName = string.Format(db.Config.TriggerNameTemplate, "insert", tableName);
            DeleteTriggerName = string.Format(db.Config.TriggerNameTemplate, "delete", tableName);
            UpdateTriggerName = string.Format(db.Config.TriggerNameTemplate, "update", tableName);
            
            SetupDatabaseSchema(tableName, db);
        }

        private void SetupDatabaseSchema(string tableName, Database db)
        {
            db.Config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var transaction = con.BeginTransaction())
                {
                    
                    transaction.CreateTriggerDelete(DeleteTriggerName, tableName, db.Conversation.ToString(), db.MessageTypeName);
                    transaction.CreateTriggerInsert(InsertTriggerName, tableName, db.Conversation.ToString(), db.MessageTypeName);
                    transaction.CreateTriggerUpdate(UpdateTriggerName, tableName, db.Conversation.ToString(), db.MessageTypeName);
                    transaction.Commit();
                }
            });
        }
        
        internal void OnReceive(XElement result)
        {
            if (Schema.TryGetEventRows(result, out IEnumerable<Row> rows, out string type))
            {
                OnOnTableDataChanged(new TableDataChangedEventArgs(rows, type));
            }
        }

        private void OnOnTableDataChanged(TableDataChangedEventArgs e)
        {
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
