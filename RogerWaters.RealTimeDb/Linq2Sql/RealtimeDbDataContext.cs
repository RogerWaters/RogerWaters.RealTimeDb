using System;
using System.CodeDom;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Linq;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using RogerWaters.RealTimeDb.Configuration;
using RogerWaters.RealTimeDb.SqlObjects;
using RogerWaters.RealTimeDb.SqlObjects.Queries;

namespace RogerWaters.RealTimeDb.Linq2Sql
{
    /// <summary>
    /// Context to query Linq DataContext as realtime query
    /// </summary>
    /// <typeparam name="T">Type of Linq DataContext</typeparam>
    public sealed class RealtimeDbDataContext<T> : IDisposable where T: DataContext
    {
        private readonly RealtimeDbDataContextBuilder<T> _contextConfiguration;
        
        private static readonly ConcurrentDictionary<Type,Delegate> _mappingMethods = new ConcurrentDictionary<Type, Delegate>();

        /// <summary>
        /// Get or Set the behavior how Dispose is applied to the database
        /// </summary>
        public DisposeBehavior DisposeBehavior { get; set; } = DisposeBehavior.CleanupSchema;

        public DatabaseConfig DatabaseConfig { get; }

        /// <summary>
        /// The internal database that controls the realtime objects
        /// </summary>
        private readonly Database _db;
        
        public RealtimeDbDataContext(RealtimeDbDataContextBuilder<T> contextConfiguration)
        {
            _contextConfiguration = contextConfiguration;
            DatabaseConfig = ((DatabaseConfigBuilder) contextConfiguration).Build();
            
            using (var context = contextConfiguration.ContextFactory())
            {
                _db = new Database(((DatabaseConfigBuilder) contextConfiguration).Build());
                foreach (var table in context.Mapping.GetTables())
                {
                    _db.GetOrAddTable(table.TableName);
                }
            }
        }

        /// <summary>
        /// Execute a query that is refreshed if dependencies change
        /// </summary>
        /// <typeparam name="TResult">Type of rows derived from </typeparam>
        /// <typeparam name="TKey">Type of key used to identify <typeparamref name="TResult"/></typeparam>
        /// <param name="query">Function that queries Linq to Sql</param>
        /// <param name="keySelector">Function to extract the key from from <typeparamref name="TResult"/></param>
        /// <returns>The query created</returns>
        public async Task<MappedSqlCachedQuery<TResult,TKey>> Query<TResult,TKey>(Func<T, IQueryable<TResult>> query, Expression<Func<TResult,TKey>> keySelector)
        {
            return await Task.Run(() =>
            {
                using (var context = _contextConfiguration.ContextFactory())
                {
                    var queryable = query(context);

                    var primaries = keySelector.GetMembers();

                    var command = context.GetCommand(queryable);
                    var text = command.CommandText;
                    foreach (var parameter in command.Parameters.OfType<SqlParameter>())
                    {
                        var name = parameter.ToString();
                        if (name.StartsWith("@", StringComparison.InvariantCulture) == false)
                        {
                            name = "@" + name;
                        }

                        text = text.Replace(name, ToInlineValue(parameter));
                    }

                    var type = typeof(TResult);
                    return _db.CustomQuery
                    (
                        text,
                        r =>
                        {
                            var mapper = (Func<Row,TResult>)_mappingMethods.GetOrAdd(type, t =>
                            {
                                var rowParam = Expression.Parameter(typeof(Row));

                                var @new = Expression.New(type.GetConstructors().First(),
                                    r.Schema.ColumnTypes.Select
                                    (
                                        kvp => Expression.Convert(
                                            Expression.Property(rowParam, "Item", Expression.Constant(kvp.Key)),
                                            kvp.Value)
                                    ));
                                return Expression.Lambda<Func<Row, TResult>>(@new, rowParam).Compile();
                            });

                            return mapper(r);
                        },
                        keySelector.Compile(),
                        s => EqualityComparer<TKey>.Default,
                        primaries.First(),
                        primaries.Skip(1).ToArray()
                    );
                }
            });
        }

        /// <summary>
        /// Convert a query parameter to inline value
        /// </summary>
        /// <param name="parameter">The parameter to extract inline value</param>
        /// <returns>The escaped inline value</returns>
        private string ToInlineValue(SqlParameter parameter)
        {
            if (parameter.Value == null || parameter.Value == DBNull.Value)
            {
                return "NULL";
            }

            switch (parameter.SqlDbType)
            {
                case SqlDbType.BigInt:
                case SqlDbType.Int:
                case SqlDbType.TinyInt:
                case SqlDbType.SmallInt:
                case SqlDbType.Decimal:
                case SqlDbType.Float:
                case SqlDbType.Money:
                case SqlDbType.SmallMoney:
                case SqlDbType.Real:
                    return parameter.Value.ToString();
                case SqlDbType.Bit:
                    if (parameter.Value is bool b)
                    {
                        return b ? "1" : "0";
                    }
                    return parameter.Value.ToString();
                case SqlDbType.Char:
                case SqlDbType.NChar:
                case SqlDbType.NText:
                case SqlDbType.NVarChar:
                case SqlDbType.UniqueIdentifier:
                case SqlDbType.Text:
                case SqlDbType.VarChar:
                    return $"N'{parameter.Value.ToString().Replace("'", "''")}'";
                case SqlDbType.DateTime:
                case SqlDbType.DateTime2:
                case SqlDbType.SmallDateTime:
                case SqlDbType.DateTimeOffset:
                    return $"N'{(DateTime)parameter.Value:yyyy-MM-dd HH:mm:ss}'";
                case SqlDbType.Date:
                    return $"N'{(DateTime)parameter.Value:yyyy-MM-dd}'";
                case SqlDbType.Time:
                    return $"N'{(DateTime)parameter.Value:HH:mm:ss}'";
                case SqlDbType.Variant:
                case SqlDbType.Xml:
                case SqlDbType.Udt:
                case SqlDbType.Structured:
                case SqlDbType.Binary:
                case SqlDbType.Image:
                case SqlDbType.Timestamp:
                case SqlDbType.VarBinary:
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _db?.Dispose();
        }
    }
}

