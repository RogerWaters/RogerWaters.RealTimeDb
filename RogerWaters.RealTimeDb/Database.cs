using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RogerWaters.RealTimeDb
{
    public sealed class Database : SchemaObject, IDisposable
    {
        internal DatabaseConfig Config { get; }
        public string ContractName { get; }
        public string MessageTypeName { get; }

        private readonly ConcurrentDictionary<string,Table> _tables = new ConcurrentDictionary<string, Table>();
        private readonly ConcurrentDictionary<string,View> _views = new ConcurrentDictionary<string, View>();
        private readonly List<CustomQuery> _customQueries = new List<CustomQuery>();

        private readonly Thread _messageReader;
        private volatile bool _refreshTasks = true;
        private volatile bool _disposed = false;
        
        public IEnumerable<Table> Tables => _tables.Values.Where(t => t.Hidden == false);
        public IEnumerable<View> Views => _views.Values.Where(v => v.Hidden == false);

        public Database(DatabaseConfig config)
        {
            Config = config;
            MessageTypeName = config.MessageTypeName;
            ContractName = config.ContractName;

            config.DatabaseConnectionString.EnableBroker();

            config.DatabaseConnectionString.WithConnection(con =>
            {
                using (var transaction = con.BeginTransaction())
                {
                    transaction.CreateMessageType(MessageTypeName);
                    transaction.CreateContract(ContractName, MessageTypeName);
                    transaction.Commit();
                }
            });

            _messageReader = new Thread(StartListen);
            _messageReader.Start();
        }
        
        private void StartListen()
        {
            while (_refreshTasks)
            {
                _refreshTasks = false;
                var tables = _tables.Values.ToList();
                Task<IEnumerable<string>>[] tasks = new Task<IEnumerable<string>>[_tables.Count];
                for (var i = 0; i < tables.Count; i++)
                {
                    tasks[i] = Config.DatabaseConnectionString.ReceiveMessagesAsync(tables[i].ReceiverQueueName, TimeSpan.FromSeconds(5));
                }

                while (_refreshTasks == false)
                {
                    var id = Task.WaitAny(tasks, TimeSpan.FromMinutes(1));
                    if (id > -1)
                    {
                        tables[id].OnReceive(tasks[id].Result);
                        tasks[id] = Config.DatabaseConnectionString.ReceiveMessagesAsync(tables[id].ReceiverQueueName, TimeSpan.FromSeconds(30));
                    }
                }
                for (var i = 0; i < tasks.Length; i++)
                {
                    tables[i].OnReceive(tasks[i].Result);
                }
            }
            
        }

        public CustomQuery CustomQuery(UserQuery query)
        {
            return new CustomQuery(this, query);
        }

        internal void AddTable(Table table)
        {
            if (!_tables.TryAdd(table.TableName,table))
            {
                throw new InvalidOperationException($"Table with Name {table.TableName} already in Collection");
            }
            _refreshTasks = true;
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
            if (_tables.TryRemove(table.TableName, out Table t))
            {
                
            }
        }

        internal void RemoveView(View view)
        {
            if (_views.TryRemove(view.ViewName, out View v))
            {

            }
        }

        public Table GetOrAddTable(string tableName)
        {
            return _tables.GetOrAdd(tableName, t =>
            {
                _refreshTasks = true;
                return new Table(this, tableName);
            });
        }

        public View GetOrAddView(string viewName, string primaryKeyColumn, params string[] primaryKeyColumns)
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
                    transaction.DropContract(ContractName);
                    transaction.DropMessageType(MessageTypeName);
                    transaction.Commit();
                }
            });
        }
    }
}
