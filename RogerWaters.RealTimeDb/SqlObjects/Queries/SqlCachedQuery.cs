using System.Collections;
using System.Linq;
using RogerWaters.RealTimeDb.EventArgs;

namespace RogerWaters.RealTimeDb.SqlObjects.Queries
{
    /// <summary>
    /// Query that encapsulate an Sql-Statement
    /// </summary>
    internal sealed class SqlCachedQuery : MappedSqlCachedQuery<Row, object[]>
    {
        public SqlCachedQuery(Database db, string query, string primaryKeyColumn, params string[] additionalPrimaryKeyColumns) 
            : base(db, query, r => r, r => r.Key, s => s.RowKeyEqualityComparer, primaryKeyColumn, additionalPrimaryKeyColumns)
        {
        }
    }
}
