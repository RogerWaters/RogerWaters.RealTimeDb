using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RogerWaters.RealTimeDb.SqlObjects.Caching
{
    internal sealed class QueryView:IDisposable
    {
        private readonly Database _db;
        public readonly string Query;
        public readonly string[] PrimaryKeyColumns;
        public readonly Guid Guid = Guid.NewGuid();
        public readonly SqlObjectName ViewName;
        

        public QueryView(Database db, string query, string[] primaryKeyColumns)
        {
            _db = db;
            Query = query;
            PrimaryKeyColumns = primaryKeyColumns;
            ViewName = string.Format(db.Config.QueryViewNameTemplate,Guid.ToString().Replace('-','_'));
            db.Config.DatabaseConnectionString.WithConnection
            (
                con =>
                {
                    using (var command = con.CreateCommand())
                    {
                        command.CommandText = $"CREATE VIEW {ViewName} AS {Environment.NewLine}{query}";
                        command.ExecuteNonQuery();
                    }
                }
            );
        }

        
        public void Dispose()
        {
            _db.Config.DatabaseConnectionString.WithConnection
            (
                con =>
                {
                    using (var command = con.CreateCommand())
                    {
                        command.CommandText = $"DROP VIEW {ViewName}";
                        command.ExecuteNonQuery();
                    }
                }
            );
        }
    }
}
