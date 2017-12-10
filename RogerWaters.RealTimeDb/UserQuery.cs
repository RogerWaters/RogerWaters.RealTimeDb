using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RogerWaters.RealTimeDb
{
    public abstract class UserQuery
    {
        public abstract string CommandText { get; }
        public abstract string PrimaryKeyColumn { get; }
        public abstract string[] AdditionalPrimaryKeyColumns { get; }

        public abstract void Initialize(SqlDataReader obj);

        public abstract void RowsInserted(IReadOnlyList<Row> rows);

        public abstract void RowsUpdated(IReadOnlyList<Row> rows);

        public abstract void RowsDeleted(IReadOnlyList<Row> rows);
    }
}
