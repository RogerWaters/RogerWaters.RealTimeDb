using RogerWaters.RealTimeDb.SqlObjects;

namespace RogerWaters.RealTimeDb.Configuration
{
    /// <summary>
    /// Configuration to setup all values for observer
    /// </summary>
    public sealed class DatabaseConfig
    {
        /// <summary>
        /// Connection string to database
        /// </summary>
        public string DatabaseConnectionString { get; }

        /// <summary>
        /// Template used to generate receiver queue name
        /// </summary>
        public SqlObjectName ReceiverQueueName { get; }

        /// <summary>
        /// Template used to generate sender queue name
        /// </summary>
        public SqlObjectName SenderQueueName { get; }

        /// <summary>
        /// Template used to generate receiver service name
        /// </summary>
        public SqlSchemalessObjectName ReceiverServiceName { get; }

        /// <summary>
        /// Template used to generate sender service name
        /// </summary>
        public SqlSchemalessObjectName SenderServiceName { get; }

        /// <summary>
        /// Name that will be used as the message type
        /// </summary>
        public SqlSchemalessObjectName MessageTypeName { get; }

        /// <summary>
        /// Name that will be used as the contract name for conversation
        /// </summary>
        public SqlSchemalessObjectName ContractName { get; }

        /// <summary>
        /// Template to generate trigger objects
        /// </summary>
        /// <remarks>
        /// 0: schema of table<br/>
        /// 1: action (insert, update, delete)<br/>
        /// 2: name of the table
        /// </remarks>
        public SqlObjectName TriggerNameTemplate { get; }

        /// <summary>
        /// Template to generate query views, to observer queries
        /// </summary>
        /// <remarks>
        /// 0: Guid applied to the query
        /// </remarks>
        public SqlObjectName QueryViewNameTemplate { get; }

        /// <summary>
        /// Template to generate table for caching view result
        /// </summary>
        /// <remarks>
        /// 0: schema of the view<br/>
        /// 1: name of the view
        /// </remarks>
        public SqlObjectName ViewCacheTableNameTemplate { get; }

        /// <summary>
        /// If not null use memory tables for view and query caches
        /// </summary>
        public string MemoryTableStoragePath { get; }

        /// <summary>
        /// If views and triggers will be compiled with sql native
        /// </summary>
        public bool CompileObjects { get; }

        /// <summary>
        /// Creates a new Configuration
        /// </summary>
        /// <param name="builder">The builder that contains all the configurations</param>
        internal DatabaseConfig(DatabaseConfigBuilder builder)
        {
            DatabaseConnectionString = builder.ConnectionString;
            ReceiverQueueName = builder.ReceiverQueueName;
            SenderQueueName = builder.SenderQueueName;
            ReceiverServiceName = builder.ReceiverServiceName;
            SenderServiceName = builder.SenderServiceName;
            MessageTypeName = builder.MessageTypeName;
            ContractName = builder.ContractName;
            TriggerNameTemplate = builder.TriggerNameTemplate;
            QueryViewNameTemplate = builder.QueryViewNameTemplate;
            ViewCacheTableNameTemplate = builder.ViewCacheTableNameTemplate;
            MemoryTableStoragePath = builder.MemoryTableStoragePath;
            CompileObjects = builder.CompileObjects;
        }
    }
}
