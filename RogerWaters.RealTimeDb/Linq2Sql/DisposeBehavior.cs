using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RogerWaters.RealTimeDb.Linq2Sql
{
    /// <summary>
    /// The behavior how db-schema is disposed
    /// </summary>
    public enum DisposeBehavior
    {
        /// <summary>
        /// Removes all created objects
        /// </summary>
        CleanupSchema,
        /// <summary>
        /// Removes only query schema changes, but keeps all events
        /// </summary>
        KeepChanges
    }
}
