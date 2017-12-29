using RogerWaters.RealTimeDb.SqlObjects;

namespace RogerWaters.RealTimeDb.Configuration
{
    /// <summary>
    /// Builder to setup all settings configurable for database
    /// </summary>
    public class DatabaseConfigBuilder
    {
        /// <summary>
        /// Connection string to database
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        /// Template used to generate receiver queue name
        /// </summary>
        public SqlObjectName ReceiverQueueName { get; set; } = "tcp://RogerWaters/RealTimeDb/Receiver";

        /// <summary>
        /// Template used to generate sender queue name
        /// </summary>
        public SqlObjectName SenderQueueName { get; set; } = "tcp://RogerWaters/RealTimeDb/Sender";

        /// <summary>
        /// Template used to generate receiver service name
        /// </summary>
        public SqlSchemalessObjectName ReceiverServiceName { get; set; } = "tcp://RogerWaters/RealTimeDb/ReceiverService";

        /// <summary>
        /// Template used to generate sender service name
        /// </summary>
        public SqlSchemalessObjectName SenderServiceName { get; set; } = "tcp://RogerWaters/RealTimeDb/SenderService";

        /// <summary>
        /// Name that will be used as the message type
        /// </summary>
        public SqlSchemalessObjectName MessageTypeName { get; set; } = "RogerWaters.RealTimeDb.MessageType";

        /// <summary>
        /// Name that will be used as the contract name for conversation
        /// </summary>
        public SqlSchemalessObjectName ContractName { get; set; } = "RogerWaters.RealTimeDb.Contract";

        /// <summary>
        /// Template to generate trigger objects
        /// </summary>
        /// <remarks>
        /// 0: schema of table<br/>
        /// 1: action (insert, update, delete)<br/>
        /// 2: name of the table
        /// </remarks>
        public SqlObjectName TriggerNameTemplate { get; set; } = "{0}.TR_RogerWaters_RealTimeDb_{1}_{2}";

        /// <summary>
        /// Template to generate query views, to observer queries
        /// </summary>
        /// <remarks>
        /// 0: Guid applied to the query
        /// </remarks>
        public SqlObjectName QueryViewNameTemplate { get; set; } = "dbo.vw_RDB_{0}";

        /// <summary>
        /// Template to generate table for caching view result
        /// </summary>
        /// <remarks>
        /// 0: schema of the view<br/>
        /// 1: name of the view
        /// </remarks>
        public SqlObjectName ViewCacheTableNameTemplate { get; set; } = "{0}.tmp_RDB_{1}";

        /// <summary>
        /// If not null use memory tables for view and query caches
        /// </summary>
        public string MemoryTableStoragePath { get; set; } = null;

        /// <summary>
        /// If views and triggers will be compiled with sql native
        /// </summary>
        public bool CompileObjects { get; set; } = true;

        /// <summary>
        /// Creates a new Configuration
        /// </summary>
        /// <param name="connectionString">The connection used to connect to SqlServer</param>
        public DatabaseConfigBuilder(string connectionString)
        {
            ConnectionString = connectionString;
        }

        /// <summary>
        /// Build the configuration from the current state
        /// </summary>
        /// <returns></returns>
        public DatabaseConfig Build()
        {
            return new DatabaseConfig(this);
        }
    }
}