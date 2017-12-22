using RogerWaters.RealTimeDb.SqlObjects;

namespace RogerWaters.RealTimeDb
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
        public SqlObjectName ReceiverQueueNameTemplate { get; set; } = "tcp://RogerWaters/RealTimeDb/Receiver";

        /// <summary>
        /// Template used to generate sender queue name
        /// </summary>
        public SqlObjectName SenderQueueNameTemplate { get; set; } = "tcp://RogerWaters/RealTimeDb/Sender";

        /// <summary>
        /// Template used to generate receiver service name
        /// </summary>
        public SqlSchemalessObjectName ReceiverServiceNameTemplate { get; set; } = "tcp://RogerWaters/RealTimeDb/ReceiverService";

        /// <summary>
        /// Template used to generate sender service name
        /// </summary>
        public SqlSchemalessObjectName SenderServiceNameTemplate { get; set; } = "tcp://RogerWaters/RealTimeDb/SenderService";

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
        /// Creates a new Configuration
        /// </summary>
        /// <param name="connectionString">The connection used to connect to SqlServer</param>
        public DatabaseConfig(string connectionString)
        {
            DatabaseConnectionString = connectionString;
        }
    }
}
