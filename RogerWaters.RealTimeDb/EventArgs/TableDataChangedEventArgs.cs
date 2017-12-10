using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RogerWaters.RealTimeDb.EventArgs
{
    public sealed class TableDataChangedEventArgs:System.EventArgs
    {
        public string ChangeType { get; }
        public IReadOnlyList<Row> Rows { get; }

        public TableDataChangedEventArgs(IEnumerable<Row> rows, string changeType)
        {
            ChangeType = changeType;
            Rows = rows.ToList();
        }
    }
}
