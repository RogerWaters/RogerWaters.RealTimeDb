using System.Collections.Generic;

namespace RogerWaters.RealTimeDb.EventArgs
{
    /// <summary>
    /// EventArgs for changed table rows
    /// </summary>
    public sealed class TableDataChangedEventArgs : System.EventArgs
    {
        /// <summary>
        /// Kind of change that created the event
        /// </summary>
        public RowChangeKind ChangeKind { get; }

        /// <summary>
        /// The rows affected
        /// </summary>
        public IReadOnlyList<Row> Rows { get; }

        /// <summary>
        /// Creates a new instance of <see cref="TableDataChangedEventArgs"/>
        /// </summary>
        /// <param name="rows">The rows affected</param>
        /// <param name="changeKind">Kind of change that created the event</param>
        public TableDataChangedEventArgs(IReadOnlyList<Row> rows, RowChangeKind changeKind)
        {
            ChangeKind = changeKind;
            Rows = rows;
        }
    }
}
