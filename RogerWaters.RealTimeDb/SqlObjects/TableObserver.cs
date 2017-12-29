using System;
using System.Xml.Linq;
using RogerWaters.RealTimeDb.EventArgs;

namespace RogerWaters.RealTimeDb.SqlObjects
{
    /// <summary>
    /// Observe a table for changes
    /// </summary>
    internal sealed class TableObserver : SchemaObject, IDisposable
    {
        /// <summary>
        /// The database containing this table
        /// </summary>
        private readonly Database _db;

        /// <summary>
        /// The schema for this table
        /// </summary>
        public RowSchema Schema { get; }

        /// <summary>
        /// The name of this table in Sql-Server
        /// </summary>
        public SqlObjectName SqlObjectName { get; }

        /// <summary>
        /// The name of the trigger observing updates
        /// </summary>
        private readonly SqlObjectName _updateTriggerName;

        /// <summary>
        /// The name of the trigger observing deletes
        /// </summary>
        private readonly SqlObjectName _deleteTriggerName;

        /// <summary>
        /// The name of the trigger observing inserts
        /// </summary>
        private readonly SqlObjectName _insertTriggerName;
        
        /// <summary>
        /// Occurs if row changes are detected
        /// </summary>
        public event EventHandler<TableDataChangedEventArgs> OnTableDataChanged;

        /// <summary>
        /// Initialize a new instance of <see cref="TableObserver"/>
        /// </summary>
        /// <param name="db">The database that contains table</param>
        /// <param name="sqlObjectName">The name of the table</param>
        public TableObserver(Database db, SqlObjectName sqlObjectName)
        {
            _db = db;
            SqlObjectName = sqlObjectName;
            Schema = new RowSchema(db.Config.DatabaseConnectionString, sqlObjectName);

            _insertTriggerName = string.Format(db.Config.TriggerNameTemplate, sqlObjectName.Schema, "insert", sqlObjectName.Name);
            _deleteTriggerName = string.Format(db.Config.TriggerNameTemplate, sqlObjectName.Schema, "delete", sqlObjectName.Name);
            _updateTriggerName = string.Format(db.Config.TriggerNameTemplate, sqlObjectName.Schema, "update", sqlObjectName.Name);

            SetupDatabaseSchema();
        }

        /// <summary>
        /// Setup database to receive changes for table
        /// </summary>
        private void SetupDatabaseSchema()
        {
            var config = _db.Config;
            var dialogHandle = _db.ConversationHandle.ToString();

            config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var transaction = con.BeginTransaction())
                {
                    transaction.CreateTriggerDelete(_deleteTriggerName, SqlObjectName, dialogHandle, config.MessageTypeName);
                    transaction.CreateTriggerInsert(_insertTriggerName, SqlObjectName, dialogHandle, config.MessageTypeName);
                    transaction.CreateTriggerUpdate(_updateTriggerName, SqlObjectName, dialogHandle, config.MessageTypeName);
                    transaction.Commit();
                }
            });
        }

        /// <summary>
        /// Called if the database receive a message for this table
        /// </summary>
        /// <param name="result">The event data</param>
        internal void OnReceive(XElement result)
        {
            if (Schema.TryGetEventRows(result, out var rows, out var changeKind))
            {
                OnTableDataChanged?.Invoke(this, new TableDataChangedEventArgs(rows, changeKind));
            }
        }

        /// <inheritdoc />
        public override void CleanupSchemaChanges()
        {
            var config = _db.Config;
            config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var transaction = con.BeginTransaction())
                {
                    transaction.DropTrigger(_deleteTriggerName);
                    transaction.DropTrigger(_insertTriggerName);
                    transaction.DropTrigger(_updateTriggerName);

                    transaction.Commit();
                }
            });
        }

        public void Dispose()
        {
            CleanupSchemaChanges();
        }
    }
}