using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RogerWaters.RealTimeDb.Configuration
{
    /// <summary>
    /// Policy where the current state of result is stored
    /// </summary>
    public sealed class CachingPolicy
    {
        /// <summary>
        /// If enabled view changes are calculated in memory
        /// </summary>
        /// <remarks>
        /// This reduce the load in Sql-Server but increase load of application
        /// </remarks>
        public bool CalculateViewChangesInMemory { get; set; } = false;

        /// <summary>
        /// If enabled query changes are calculated in memory
        /// </summary>
        /// <remarks>
        /// This reduce the load in Sql-Server but increase load of application
        /// </remarks>
        public bool CalculateQueryChangesInMemory { get; set; } = true;

        /// <summary>
        /// IF enabled stores the current state of the result in memory
        /// </summary>
        /// <remarks>
        /// This increase the memory usage but allows fast access to rapidly queried results
        /// </remarks>
        public bool StoreQueryStateInMemory { get; set; } = true;
    }
}
