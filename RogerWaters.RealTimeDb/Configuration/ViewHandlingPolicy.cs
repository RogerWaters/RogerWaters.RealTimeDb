using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RogerWaters.RealTimeDb.Configuration
{
    /// <summary>
    /// Tells how views get observed
    /// </summary>
    public enum ViewHandlingPolicy
    {
        /// <summary>
        /// Create a temporary table for the view that is merged in Sql-Server<br/>
        /// Can increase load on Sql-Server<br/>
        /// Can result in high latency
        /// </summary>
        /// <remarks>
        /// Supported by any version of Sql-Server since 2008R2
        /// </remarks>
        MergeWithSqlTable,
        
        /// <summary>
        /// Create a temporary in memory table for the view that is merged in Sql-Server<br/>
        /// Can increase load on Sql-Server<br/>
        /// Increases memory usage in Sql-Server
        /// </summary>
        /// <remarks>
        /// Supported by any version of Sql-Server since 2017
        /// </remarks>
        MergeWithSqlMemoryTable,
        
        /// <summary>
        /// Copy the result of view into memory and detect changes there<br/>
        /// Increase memory usage of Application
        /// </summary>
        /// <remarks>
        /// Supported by any version of Sql-Server since 2005
        /// </remarks>
        MergeInMemory
    }
}
