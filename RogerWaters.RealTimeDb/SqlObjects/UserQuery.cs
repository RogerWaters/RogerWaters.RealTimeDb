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
        /// The sql that reptrsents the query
        /// </summary>
        public abstract string CommandText { get; }

        /// <summary>
        /// Columnname that is used as primary key
        /// </summary>
        public abstract string PrimaryKeyColumn { get; }

        /// <summary>
        /// Additional columns that make primary key unique
        /// </summary>
        public abstract string[] AdditionalPrimaryKeyColumns { get; }

        /// <summary>
        /// Initially called to allow internal cache build
        /// </summary>
        /// <param name="obj">The reader that contains all Data from the query</param>
        /// <param name="schema">The schema that explains the data from reader</param>
        public abstract void Initialize(SqlDataReader obj, RowSchema schema);

        /// <summary>
        /// Called for every batch of rows that are inserted
        /// </summary>
        /// <param name="rows">The rows that are inserted</param>
        public abstract void RowsInserted(IReadOnlyList<Row> rows);

        
        /// <summary>
        /// Called for every batch of rows that are updated
        /// </summary>
        /// <param name="rows">The rows that are updatd</param>
        public abstract void RowsUpdated(IReadOnlyList<Row> rows);

        
        /// <summary>
        /// Called for every batch of rows that are deleted
        /// </summary>
        /// <param name="rows">The rows that are deleted</param>
        public abstract void RowsDeleted(IReadOnlyList<Row> rows);
    }
}
