using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;
using RogerWaters.RealTimeDb.Configuration;
using RogerWaters.RealTimeDb.SqlObjects.Queries;

namespace RogerWaters.RealTimeDb.SqlObjects
{
    /// <summary>
    /// Represents a db that is prepared for synchronization
    /// </summary>
    internal sealed class Database : IDisposable
    {
        /// <summary>
        /// The configuration the db is initialized with
        /// </summary>
        public DatabaseConfig Config { get; }

        /// <summary>
        /// The handle to transmit messages from Sql-Server to <see cref="Database"/>
        /// </summary>
        public Guid ConversationHandle => _messageTransmitter.ConversationHandle;

        /// <summary>
        /// Tables currently observed
        /// </summary>
        private readonly ReferenceSourceCollcetion<SqlObjectName, TableObserver> _tables = new ReferenceSourceCollcetion<SqlObjectName, TableObserver>();
        
        /// <summary>
        /// Custom queries currently active
        /// </summary>
        private readonly ConcurrentDictionary<Guid,  QueryObserver> _customQueries = new ConcurrentDictionary<Guid,  QueryObserver>();
        
        /// <summary>
        /// Transmitter that receives messages from the queue
        /// </summary>
        private readonly MessageTransmitter _messageTransmitter;

        private readonly LockedDisposeHelper _disposeHelper = new LockedDisposeHelper();

        /// <summary>
        /// Initialize a new instance of <see cref="Database"/>
        /// </summary>
        /// <param name="config">The configuration to setup database</param>
        public Database(DatabaseConfig config)
        {
            Config = config;
            
            config.DatabaseConnectionString.EnableBroker();

            _messageTransmitter = new MessageTransmitter(this);
            _messageTransmitter.MessageRecieved += OnMessageRecieved;

            _disposeHelper.Attach(_messageTransmitter);
            _disposeHelper.Attach(_tables);
            _disposeHelper.Attach(() =>
            {
                var queries = _customQueries.ToArray();
                _customQueries.Clear();
                queries.AsParallel().ForAll(q => q.Value.Dispose());
            });
        }
        
        /// <summary>
        /// Executed if message is received from Sql-Server
        /// </summary>
        /// <param name="root">The message in valid XML form</param>
        private void OnMessageRecieved(XElement root)
        {
            if (_tables.TryCreateReference(root.Name.LocalName, out var reference))
            {
                var entry = root.Elements().FirstOrDefault();
                if (entry != null)
                {
                    reference.Value.OnReceive(entry);
                }
                reference.Dispose();
            }
        }

        internal SqlCachedQuery CustomQuery(string query, string primaryKeyColumn, params string[] additionalPrimaryKeyColumns)
        {
            var customQuery = new SqlCachedQuery(this, query, Config.CachingType, primaryKeyColumn, additionalPrimaryKeyColumns);
            return _disposeHelper.DoOrThrow(() =>
            {
                _customQueries.TryAdd(customQuery.Guid, customQuery);
                return customQuery;
            });
        }

        internal MappedSqlCachedQuery<TRow, TKey> CustomQuery<TRow, TKey>(string query, Func<Row, TRow> rowFactory,
            Func<TRow, TKey> keyExtractor, Func<RowSchema, IEqualityComparer<TKey>> keyComparerFactory,
            string primaryKeyColumn, params string[] additionalPrimaryKeyColumns)
        {
            var customQuery = new MappedSqlCachedQuery<TRow, TKey>(this, query, rowFactory, keyExtractor, keyComparerFactory, Config.CachingType, primaryKeyColumn, additionalPrimaryKeyColumns);
            return _disposeHelper.DoOrThrow(() =>
            {
                _customQueries.TryAdd(customQuery.Guid, customQuery);
                return customQuery;
            });
        }
        
        public IReference<TableObserver> GetOrAddTable(SqlObjectName sqlObjectName)
        {
            return _disposeHelper.DoOrThrow(() => _tables.GetOrCreate(sqlObjectName, t => new TableObserver(this, sqlObjectName)));
        }

        public void Dispose()
        {
            _disposeHelper.Dispose();
        }

        internal IReference<TableObserver> GetOrAddTable(SqlObjectName sqlObjectName, string[] primaryKeyColumns, bool isMemoryTable)
        {
            return _disposeHelper.DoOrThrow(() => _tables.GetOrCreate(sqlObjectName, t => new TableObserver(this, sqlObjectName, primaryKeyColumns,isMemoryTable)));
        }
    }
}
