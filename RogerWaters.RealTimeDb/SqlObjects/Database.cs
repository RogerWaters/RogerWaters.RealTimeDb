using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Xml.Linq;

namespace RogerWaters.RealTimeDb.SqlObjects
{
    /// <summary>
    /// Represents a db that is prepared for synchhronization
    /// </summary>
    internal sealed class Database : SchemaObject, IDisposable
    {
        /// <summary>
        /// The configuration the db is initialized with
        /// </summary>
        internal DatabaseConfig Config { get; }

        /// <summary>
        /// The name of the contract used for message exchange
        /// </summary>
        public SqlSchemalessObjectName ContractName { get; }

        /// <summary>
        /// 
        /// </summary>
        public SqlSchemalessObjectName MessageTypeName { get; }

        public Guid Conversation { get; }

        public SqlSchemalessObjectName ReceiverServiceName { get; }

        public SqlSchemalessObjectName SenderServiceName { get; }

        public SqlObjectName ReceiverQueueName { get; }

        public SqlObjectName SenderQueueName { get; }

        private readonly ConcurrentDictionary<SqlObjectName, Table> _tables = new ConcurrentDictionary<SqlObjectName, Table>();
        private readonly ConcurrentDictionary<SqlObjectName, View> _views = new ConcurrentDictionary<SqlObjectName, View>();
        private readonly List<CustomQuery> _customQueries = new List<CustomQuery>();

        private readonly Thread _messageReader;
        private volatile bool _disposed = false;
        
        public Database(DatabaseConfig config)
        {
            Config = config;
            MessageTypeName = config.MessageTypeName;
            ContractName = config.ContractName;
            SenderQueueName = Config.SenderQueueNameTemplate;
            ReceiverQueueName = Config.ReceiverQueueNameTemplate;
            SenderServiceName = Config.SenderServiceNameTemplate;
            ReceiverServiceName = Config.ReceiverServiceNameTemplate;

            Guid conversation = Guid.Empty;

            config.DatabaseConnectionString.EnableBroker();

            config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var transaction = con.BeginTransaction())
                {
                    transaction.CreateMessageType(MessageTypeName);
                    transaction.CreateContract(ContractName, MessageTypeName);
                    transaction.CreateQueue(SenderQueueName);
                    transaction.CreateQueue(ReceiverQueueName);
                    transaction.CreateService(SenderServiceName, SenderQueueName, config.ContractName);
                    transaction.CreateService(ReceiverServiceName, ReceiverQueueName, config.ContractName);
                    conversation = transaction.GetConversation(SenderServiceName, ReceiverServiceName, config.ContractName);
                    transaction.Commit();
                }
            });
            Conversation = conversation;
            _messageReader = new Thread(StartListen);
            _messageReader.Start();
        }

        private void StartListen()
        {
            while (true)
            {
                foreach (var message in Config.DatabaseConnectionString.ReceiveMessages(ReceiverQueueName, TimeSpan.FromSeconds(5)))
                {
                    var root = XElement.Parse(message);
                    if (_tables.TryGetValue(root.Name.LocalName,out Table t))
                    {
                        var entry = root.Elements().FirstOrDefault();
                        if (entry != null)
                        {
                            t.OnReceive(entry);
                        }
                    }
                }
            }
        }

        internal CustomQuery CustomQuery(UserQuery query)
        {
            return new CustomQuery(this, query);
        }

        internal void AddTable(Table table)
        {
            if (!_tables.TryAdd(table.SqlObjectName,table))
            {
                throw new InvalidOperationException($"Table with Name {table.SqlObjectName} already in Collection");
            }
        }

        internal void AddView(View view)
        {
            if (!_views.TryAdd(view.ViewName, view))
            {
                throw new InvalidOperationException($"View with Name {view.ViewName} already in Collection");
            }
        }

        internal void AddCustomQuery(CustomQuery query)
        {
            _customQueries.Add(query);
        }

        internal void RemoveCustomQuery(CustomQuery query)
        {
            _customQueries.Remove(query);
        }

        internal void RemoveTable(Table table)
        {
            if (_tables.TryRemove(table.SqlObjectName, out Table t))
            {
                
            }
        }

        internal void RemoveView(View view)
        {
            if (_views.TryRemove(view.ViewName, out View v))
            {

            }
        }

        public Table GetOrAddTable(SqlObjectName sqlObjectName)
        {
            return _tables.GetOrAdd(sqlObjectName, t => new Table(this, sqlObjectName));
        }

        public View GetOrAddView(SqlObjectName viewName, string primaryKeyColumn, params string[] primaryKeyColumns)
        {
            return _views.GetOrAdd(viewName, v => new View(this, viewName, primaryKeyColumn, primaryKeyColumns));
        }

        public void Dispose()
        {
            if (_disposed == false)
            {
                _disposed = true;
                _messageReader.Abort();
                _messageReader.Join();
                _customQueries.ForEach(q => q.Dispose());
            }
        }

        public override void CleanupSchemaChanges()
        {
            Dispose();

            var queries = _customQueries.ToArray();
            _customQueries.Clear();
            foreach (var query in queries)
            {
                query.CleanupSchemaChanges();
            }
            var views = _views.ToArray();
            _views.Clear();
            foreach (var view in views)
            {
                view.Value.CleanupSchemaChanges();
            }
            var tables = _tables.ToArray();
            _tables.Clear();
            foreach (var table in tables)
            {
                table.Value.CleanupSchemaChanges();
            }

            Config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var transaction = con.BeginTransaction())
                {
                    transaction.EndConversation(Conversation);
                    transaction.DropService(SenderServiceName);
                    transaction.DropService(ReceiverServiceName);
                    transaction.DropQueue(SenderQueueName);
                    transaction.DropQueue(ReceiverQueueName);
                    transaction.DropContract(ContractName);
                    transaction.DropMessageType(MessageTypeName);
                    transaction.Commit();
                }
            });
        }
    }
}
