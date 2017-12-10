using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace RogerWaters.RealTimeDb
{
    public sealed class Row:IReadOnlyCollection<object>
    {
        private readonly object[] _data;
        private readonly RowSchema _schema;

        public Row(RowSchema schema)
        {
            _schema = schema;
            _data = new object[schema.ColumnCount];
        }


        public int Count => _data.Length;

        public object this[int index]
        {
            get => _data[index];
            internal set => _data[index] = value;
        }

        public IEnumerator<object> GetEnumerator()
        {
            return _data.Cast<object>().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _data.GetEnumerator();
        }
    }
}
