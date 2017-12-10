using System.Collections.Generic;

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
        /// <remarks>
        /// 0: Name of the Table
        /// </remarks>
        public string ReceiverQueueNameTemplate { get; set; } = "tcp://RogerWaters/RealTimeDb/Receiver/{0}";

        /// <summary>
        /// Template used to generate sender queue name
        /// </summary>
        /// <remarks>
        /// 0: Name of the Table
        /// </remarks>
        public string SenderQueueNameTemplate { get; set; } = "tcp://RogerWaters/RealTimeDb/Sender/{0}";

        /// <summary>
        /// Template used to generate receiver service name
        /// </summary>
        /// <remarks>
        /// 0: Name of the Table
        /// </remarks>
        public string ReceiverServiceNameTemplate { get; set; } = "tcp://RogerWaters/RealTimeDb/ReceiverService/{0}";

        /// <summary>
        /// Template used to generate sender service name
        /// </summary>
        /// <remarks>
        /// 0: Name of the Table
        /// </remarks>
        public string SenderServiceNameTemplate { get; set; } = "tcp://RogerWaters/RealTimeDb/SenderService/{0}";

        /// <summary>
        /// Name that will be used as the message type
        /// </summary>
        public string MessageTypeName { get; set; } = "RogerWaters.RealTimeDb.MessageType";

        /// <summary>
        /// Name that will be used as the contract name for conversation
        /// </summary>
        public string ContractName { get; set; } = "RogerWaters.RealTimeDb.Contract";

        /// <summary>
        /// Template to generate trigger objects
        /// </summary>
        /// <remarks>
        /// 0: action (insert, update, delete)<br/>
        /// 1: name of the table
        /// </remarks>
        public string TriggerNameTemplate { get; set; } = "TR_RogerWaters_RealTimeDb_{0}_{1}";

        /// <summary>
        /// Template to generate query views, to observer queries
        /// </summary>
        /// <remarks>
        /// 0: Guid applied to the query
        /// </remarks>
        public string QueryViewNameTemplate { get; set; } = "vw_RDB_{0}";
        
        /// <summary>
        /// Template to generate table for caching view result
        /// </summary>
        /// <remarks>
        /// 0: name of the view
        /// </remarks>
        public string ViewCacheTableNameTemplate { get; set; } = "tmp_RDB_{0}";
        
        /// <summary>
        /// Creates a new Configuration
        /// </summary>
        /// <param name="connectionString">The connection used to connect to SqlServer</param>
        public DatabaseConfig(string connectionString)
        {
            DatabaseConnectionString = connectionString;
        }

        /// <summary>
        /// This will setup your database to enable realtime events
        /// </summary>
        /// <remarks>
        /// Changes done here are not ensured to be revertable
        /// </remarks>
        public Database SetupDatabase()
        {
            return new Database(this);
        }
    }
}
