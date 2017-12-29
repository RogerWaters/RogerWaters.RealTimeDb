using System.Collections.Generic;
using System.Data.SqlClient;

namespace RogerWaters.RealTimeDb.SqlObjects
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class UserQuery
    {
        /// <summary>
        /// The sql that represents the query
        /// </summary>
        public abstract string CommandText { get; }

        /// <summary>
        /// ColumnName that is used as primary key
        /// </summary>
        public abstract string PrimaryKeyColumn { get; }

        /// <summary>
        /// Additional columns that make primary key unique
        /// </summary>
        public abstract string[] AdditionalPrimaryKeyColumns { get; }
    }
}
