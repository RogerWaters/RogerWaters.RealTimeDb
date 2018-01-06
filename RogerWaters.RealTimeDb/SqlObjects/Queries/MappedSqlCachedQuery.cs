using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using RogerWaters.RealTimeDb.SqlObjects.Caching;

namespace RogerWaters.RealTimeDb.SqlObjects.Queries
{
    /// <summary>
    /// Query that encapsulate an Sql-Statement
    /// </summary>
    public class MappedSqlCachedQuery<TRow,TKey> : QueryObserver, IEnumerable<TRow>
    {
        /// <summary>
        /// Function to extract <typeparamref name="TRow"/> from <see cref="Row"/>
        /// </summary>
        private readonly Func<Row, TRow> _rowFactory;

        /// <summary>
        /// Function to extract <typeparamref name="TKey"/> from <typeparamref name="TRow"/>
        /// </summary>
        private readonly Func<TRow, TKey> _keyExtractor;
        
        /// <summary>
        /// The lock to ensure that initialization and changes occur in order
        /// </summary>
        private readonly object _dataAccessLock = new object();

        /// <summary>
        /// Cache for rows
        /// </summary>
        private readonly ReducableDictionary<TKey, TRow> _rows;

        private readonly LockedDisposeHelper _disposeHelper = new LockedDisposeHelper();
        private readonly Cache _view;

        /// <summary>
        /// Create a new instance of <see cref="SqlCachedQuery"/>
        /// </summary>
        /// <param name="db">Database this query belongs to</param>
        /// <param name="query">UserQuery that is encapsulated by this query</param>
        /// <param name="keyExtractor">Function to extract <typeparamref name="TKey"/> from <typeparamref name="TRow"/></param>
        /// <param name="keyComparerFactory">Comparer to compare <typeparamref name="TKey"/></param>
        /// <param name="cachingType">Type of cache used to detect changes</param>
        /// <param name="primaryKeyColumn">The column used as primary key</param>
        /// <param name="additionalPrimaryKeyColumns">Additional column names for the primary key</param>
        /// <param name="rowFactory">Function to transform <see cref="Row"/> into <typeparamref name="TRow"/></param>
        internal MappedSqlCachedQuery(Database db, string query, Func<Row,TRow> rowFactory, Func<TRow,TKey> keyExtractor, Func<RowSchema, IEqualityComparer<TKey>> keyComparerFactory, CachingType cachingType, string primaryKeyColumn, params string[] additionalPrimaryKeyColumns) : base(Guid.NewGuid())
        {
            _rowFactory = rowFactory;
            _keyExtractor = keyExtractor;

            switch (cachingType)
            {
                case CachingType.InMemory:
                    _view = new InMemoryCache(db,query, new[] {primaryKeyColumn}.Union(additionalPrimaryKeyColumns).ToArray());
                    break;
                case CachingType.SqlTable:
                    _view = new SqlTableCache(db, query, new[] {primaryKeyColumn}.Union(additionalPrimaryKeyColumns).ToArray());
                    break;
                case CachingType.SqlInMemoryTable:
                    _view = new SqlMemoryTableCache(db, query, new[] {primaryKeyColumn}.Union(additionalPrimaryKeyColumns).ToArray());
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(cachingType), cachingType, null);
            }
            

            lock (_dataAccessLock)
            {
                _view.Invalidated += ViewOnInvalidated;
                _disposeHelper.Attach(() => _view.Invalidated -= ViewOnInvalidated);
                var data = _view.Initialize();
                
                _rows = new ReducableDictionary<TKey, TRow>(data.Count,keyComparerFactory(_view.Schema));
                foreach (var row in data)
                {
                    var entry = rowFactory(row);
                    var key = keyExtractor(entry);
                    _rows.Add(key, entry);
                }
                
                _disposeHelper.Attach(_view);
            }
        }

        private void ViewOnInvalidated()
        {
            var changes = _view.CalculateChanges();
            
            lock (_dataAccessLock)
            {
                foreach (var row in changes.Item1)
                {
                    var entry = _rowFactory(row);
                    var key = _keyExtractor(entry);
                    _rows.Add(key,entry);
                }
                foreach (var row in changes.Item2)
                {
                    var entry = _rowFactory(row);
                    var key = _keyExtractor(entry);
                    _rows[key] = entry;
                }
                
                _rows.RemoveAll(changes.Item3.Select(_rowFactory).Select(_keyExtractor));
            }
        }
        
        /// <inheritdoc />
        public override void Dispose()
        {
            _disposeHelper.Dispose();
        }

        /// <inheritdoc />
        public IEnumerator<TRow> GetEnumerator()
        {
            lock (_dataAccessLock)
            {
                return _rows.Values.ToList().GetEnumerator();
            }
        }

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
