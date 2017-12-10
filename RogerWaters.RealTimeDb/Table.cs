using System;
using System.Collections.Generic;
using RogerWaters.RealTimeDb.EventArgs;

namespace RogerWaters.RealTimeDb
{
    public sealed class Table : SchemaObject
    {
        private readonly Database _db;
        private readonly Guid _conversation;
        internal RowSchema Schema { get; }
        public string TableName { get; }
        public string UpdateTriggerName { get; }
        public string DeleteTriggerName { get; }
        public string InsertTriggerName { get; }
        public string ReceiverServiceName { get; }
        public string SenderServiceName { get; }
        public string ReceiverQueueName { get; }
        public string SenderQueueName { get; }
        internal bool Hidden { get; set; }

        public event EventHandler<TableDataChangedEventArgs> OnTableDataChanged;

        public Table(Database db, string tableName)
        {
            _db = db;
            TableName = tableName;
            Schema = new RowSchema(db.Config.DatabaseConnectionString,tableName);
            
            SenderQueueName = string.Format(db.Config.SenderQueueNameTemplate, tableName);
            ReceiverQueueName = string.Format(db.Config.ReceiverQueueNameTemplate, tableName);
            SenderServiceName = string.Format(db.Config.SenderServiceNameTemplate, tableName);
            ReceiverServiceName = string.Format(db.Config.ReceiverServiceNameTemplate, tableName);
            InsertTriggerName = string.Format(db.Config.TriggerNameTemplate, "insert", tableName);
            DeleteTriggerName = string.Format(db.Config.TriggerNameTemplate, "delete", tableName);
            UpdateTriggerName = string.Format(db.Config.TriggerNameTemplate, "update", tableName);
            
            _conversation = SetupDatabaseSchema(tableName, db.Config);
        }

        private Guid SetupDatabaseSchema(string tableName, DatabaseConfig config)
        {
            Guid conversation = Guid.Empty;
            config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var transaction = con.BeginTransaction())
                {
                    transaction.CreateQueue(SenderQueueName);
                    transaction.CreateQueue(ReceiverQueueName);
                    transaction.CreateService(SenderServiceName, SenderQueueName, config.ContractName);
                    transaction.CreateService(ReceiverServiceName, ReceiverQueueName, config.ContractName);
                    conversation = transaction.GetConversation(SenderServiceName, ReceiverServiceName, config.ContractName);
                    transaction.CreateTriggerDelete(DeleteTriggerName, tableName, conversation.ToString(), config.MessageTypeName);
                    transaction.CreateTriggerInsert(InsertTriggerName, tableName, conversation.ToString(), config.MessageTypeName);
                    transaction.CreateTriggerUpdate(UpdateTriggerName, tableName, conversation.ToString(), config.MessageTypeName);
                    transaction.Commit();
                }
            });
            return conversation;
        }
        
        internal void OnReceive(IEnumerable<string> result)
        {
            foreach (var s in result)
            {
                if (Schema.TryGetEventRows(s,out IEnumerable<Row> rows,out string type))
                {
                    OnOnTableDataChanged(new TableDataChangedEventArgs(rows, type));
                }
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
                    transaction.EndConversation(_conversation);
                    transaction.DropService(SenderServiceName);
                    transaction.DropService(ReceiverServiceName);
                    transaction.DropQueue(SenderQueueName);
                    transaction.DropQueue(ReceiverQueueName);
                    
                    transaction.Commit();
                }
            });
        }
    }
}
