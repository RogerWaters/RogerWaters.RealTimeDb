using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using RogerWaters.RealTimeDb.SqlObjects;

namespace RogerWaters.RealTimeDb.Linq2Sql
{
    public sealed class TypedUserQuery<T,TKey> : UserQuery, IDisposable, INotifyCollectionChanged
    {
        private static int _counter;
        private readonly int _id;
        private readonly Func<DbDataReader, IEnumerator> _mapperFunc;
        private readonly Func<T, TKey> _keyExtractor;
        private CustomQuery _innerQuery;
        public override string CommandText { get; }
        public override string PrimaryKeyColumn { get; }
        public override string[] AdditionalPrimaryKeyColumns { get; }
        private readonly ReducableDictionary<TKey,T> _data = new ReducableDictionary<TKey,T>();

        private Func<Row, T> _func;

        internal TypedUserQuery(Func<DbDataReader, IEnumerator> mapperFunc, Func<T,TKey> keyExtractor, string commandText, string primaryKeyColumn, params string[] additionalPrimaryKeyColumns)
        {
            _id = Interlocked.Increment(ref _counter);
            PrimaryKeyColumn = primaryKeyColumn;
            AdditionalPrimaryKeyColumns = additionalPrimaryKeyColumns;
            _mapperFunc = mapperFunc;
            _keyExtractor = keyExtractor;
            CommandText = commandText;
        }

        internal void SetCustomQuery(CustomQuery query)
        {
            _innerQuery = query;
        }

        public override void Initialize(SqlDataReader obj, RowSchema cacheTableSchema)
        {
            var rowParam = Expression.Parameter(typeof(Row));

            var @new = Expression.New(typeof(T).GetConstructors().First(),cacheTableSchema.ColumnTypes.Select
            (
                kvp => Expression.Convert(Expression.Property(rowParam, "Item", Expression.Constant(kvp.Key)), kvp.Value)
            ));
            _func = Expression.Lambda<Func<Row,T>>(@new,rowParam).Compile();
            
            var mapper = _mapperFunc(obj);
            while (mapper.MoveNext())
            {
                var current = (T) mapper.Current;
                _data.Add(_keyExtractor(current),current);
            }

            WriteDebug();
        }

        private void WriteDebug()
        {
            Console.WriteLine("{1:0000} Count{0}", _data.Count, _id);
        }

        public override void RowsInserted(IReadOnlyList<Row> rows)
        {
            foreach (var value in rows.Select(_func))
            {
                _data[_keyExtractor(value)] = value;
            }
            WriteDebug();
        }

        public override void RowsUpdated(IReadOnlyList<Row> rows)
        {
            foreach (var value in rows.Select(_func))
            {
                _data[_keyExtractor(value)] = value;
            }
            WriteDebug();
        }

        public override void RowsDeleted(IReadOnlyList<Row> rows)
        {
            _data.RemoveAll(rows.Select(_func).Select(_keyExtractor));
            
            WriteDebug();
        }

        public void Dispose()
        {
            _innerQuery?.Dispose();
        }

        public event NotifyCollectionChangedEventHandler CollectionChanged;
    }
}