using System.Collections;
using System.Linq;
using RogerWaters.RealTimeDb.EventArgs;
using RogerWaters.RealTimeDb.SqlObjects.Caching;

namespace RogerWaters.RealTimeDb.SqlObjects.Queries
{
    /// <summary>
    /// Query that encapsulate an Sql-Statement
    /// </summary>
    internal sealed class SqlCachedQuery : MappedSqlCachedQuery<Row, object[]>
    {
        public SqlCachedQuery(Database db, string query, CachingType cachingType, string primaryKeyColumn, params string[] additionalPrimaryKeyColumns) 
            : base(db, query, r => r, r => r.Key, s => s.RowKeyEqualityComparer, cachingType,primaryKeyColumn, additionalPrimaryKeyColumns)
        {
        }
    }
}
