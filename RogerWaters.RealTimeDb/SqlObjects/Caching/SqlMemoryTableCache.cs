namespace RogerWaters.RealTimeDb.SqlObjects.Caching
{
    internal class SqlMemoryTableCache : SqlTableCache
    {
        protected override void CreateCacheTable(string connectionString, SqlObjectName viewName, SqlObjectName cacheName, string[] primaryColumns)
        {
            connectionString.CreateMemoryViewCache(viewName,cacheName,primaryColumns);
        }

        public SqlMemoryTableCache(Database db, string query, string[] primaryKeyColumns) : base(db, query, primaryKeyColumns)
        {
        }
    }
}