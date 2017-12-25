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
    /// <summary>
    /// A table cached query that is synchronized in Sql-Server
    /// </summary>
    /// <typeparam name="T">Type of row stored in the query</typeparam>
    /// <typeparam name="TKey">Type of key used to synchronize events</typeparam>
    public sealed class TableCachedQuery<T,TKey> : UserQuery, IDisposable, INotifyCollectionChanged
    {
        /// <summary>
        /// Function to map initial data to IEnumerable of <typeparamref name="T"/>
        /// </summary>
        private readonly Func<DbDataReader, IEnumerator> _readerMapperFunc;

        /// <summary>
        /// Function to map rows to <typeparamref name="T"/>
        /// </summary>
        private Func<Row, T> _rowMapperFunc;

        /// <summary>
        /// Function to extract <typeparamref name="TKey"/> from <typeparam name="T"></typeparam>
        /// </summary>
        private readonly Func<T, TKey> _keyExtractor;

        /// <summary>
        /// The database object assotiated with this query to destroy if query is destroyed
        /// </summary>
        private CustomQuery _innerQuery;

        /// <summary>
        /// The sql that reptrsents the query
        /// </summary>
        public override string CommandText { get; }

        /// <summary>
        /// Columnname that is used as primary key
        /// </summary>
        public override string PrimaryKeyColumn { get; }

        /// <summary>
        /// Additional columns that make primary key unique
        /// </summary>
        public override string[] AdditionalPrimaryKeyColumns { get; }

        /// <summary>
        /// Cache that processes events fast an store the current state of the query
        /// </summary>
        private readonly ReducableDictionary<TKey,T> _data = new ReducableDictionary<TKey,T>();

        /// <summary>
        /// Initializes a new instance of <see cref="TableCachedQuery{T,TKey}"/>
        /// </summary>
        /// <param name="readerMapperFunc">Function to map initial data to IEnumerable of <typeparamref name="T"/></param>
        /// <param name="keyExtractor">Function to extract <typeparamref name="TKey"/> from <typeparam name="T"></typeparam></param>
        /// <param name="commandText">The sql that reptrsents the query</param>
        /// <param name="primaryKeyColumn">Columnname that is used as primary key</param>
        /// <param name="additionalPrimaryKeyColumns">Additional columns that make primary key unique</param>
        internal TableCachedQuery(Func<DbDataReader, IEnumerator> readerMapperFunc, Func<T,TKey> keyExtractor, string commandText, string primaryKeyColumn, params string[] additionalPrimaryKeyColumns)
        {
            PrimaryKeyColumn = primaryKeyColumn;
            AdditionalPrimaryKeyColumns = additionalPrimaryKeyColumns;
            _readerMapperFunc = readerMapperFunc;
            _keyExtractor = keyExtractor;
            CommandText = commandText;
        }

        /// <summary>
        /// Attach <paramref name="query"/> to close Query on dispose
        /// </summary>
        /// <param name="query">The database object that represents the sql objects created for this query</param>
        internal void SetCustomQuery(CustomQuery query)
        {
            _innerQuery = query;
        }

        /// <inheritdoc />
        public override void Initialize(SqlDataReader obj, RowSchema cacheTableSchema)
        {
            var rowParam = Expression.Parameter(typeof(Row));

            var @new = Expression.New(typeof(T).GetConstructors().First(),cacheTableSchema.ColumnTypes.Select
            (
                kvp => Expression.Convert(Expression.Property(rowParam, "Item", Expression.Constant(kvp.Key)), kvp.Value)
            ));
            _rowMapperFunc = Expression.Lambda<Func<Row,T>>(@new,rowParam).Compile();
            
            var mapper = _readerMapperFunc(obj);
            while (mapper.MoveNext())
            {
                var current = (T) mapper.Current;
                _data.Add(_keyExtractor(current),current);
            }

            WriteDebug();
        }

        /// <summary>
        /// Debug internal cache
        /// </summary>
        private void WriteDebug()
        {
            //Console.WriteLine("Count {0}", _data.Count);
        }

        /// <inheritdoc />
        public override void RowsInserted(IReadOnlyList<Row> rows)
        {
            foreach (var value in rows.Select(_rowMapperFunc))
            {
                _data[_keyExtractor(value)] = value;
            }
            WriteDebug();
        }

        /// <inheritdoc />
        public override void RowsUpdated(IReadOnlyList<Row> rows)
        {
            foreach (var value in rows.Select(_rowMapperFunc))
            {
                _data[_keyExtractor(value)] = value;
            }
            WriteDebug();
        }

        /// <inheritdoc />
        public override void RowsDeleted(IReadOnlyList<Row> rows)
        {
            _data.RemoveAll(rows.Select(_rowMapperFunc).Select(_keyExtractor));
            
            WriteDebug();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _innerQuery?.Dispose();
        }

        /// <inheritdoc />
        public event NotifyCollectionChangedEventHandler CollectionChanged;
    }
}