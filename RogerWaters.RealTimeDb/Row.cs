using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace RogerWaters.RealTimeDb
{
    public sealed class Row:IReadOnlyCollection<object>
    {
        private readonly object[] _data;
        public readonly RowSchema Schema;

        public Row(RowSchema schema)
        {
            Schema = schema;
            _data = new object[schema.ColumnCount];
        }

        public int Count => _data.Length;

        public object this[int index]
        {
            get => _data[index];
            internal set
            {
                if (value == null)
                {
                    _data[index] = DBNull.Value;
                }
                else
                {
                    _data[index] = value;
                }
            }
        }

        public object[] Key => Schema.GetKey(this);

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
