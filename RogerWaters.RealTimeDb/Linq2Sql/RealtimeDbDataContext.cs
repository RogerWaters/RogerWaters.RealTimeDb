using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Data.Linq;
using System.Data.Linq.SqlClient;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using RogerWaters.RealTimeDb.SqlObjects;

namespace RogerWaters.RealTimeDb.Linq2Sql
{
    /// <summary>
    /// Context to query Linq DataContext as realtime query
    /// </summary>
    /// <typeparam name="T">Type of Linq DataContext</typeparam>
    public sealed class RealtimeDbDataContext<T> : IDisposable where T: DataContext
    {
        /// <summary>
        /// Function to generate a preconfigured context
        /// </summary>
        private readonly Func<T> _contextFactory;

        /// <summary>
        /// Get or Set the behavior how Dispose is applied to the database
        /// </summary>
        public DisposeBehavior DisposeBehavior { get; set; } = DisposeBehavior.CleanupSchema;

        /// <summary>
        /// The internal database that controls the realtime objects
        /// </summary>
        private readonly Database _db;

        /// <summary>
        /// Create a new instance <see cref="RealtimeDbDataContext{T}"/>
        /// </summary>
        /// <param name="contextFactory">Function to generate a preconfigured context</param>
        public RealtimeDbDataContext(Func<T> contextFactory)
        {
            _contextFactory = contextFactory;
            using (var context = contextFactory())
            {
                _db = new Database(new DatabaseConfig(context.Connection.ConnectionString));
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
        public async Task<TypedUserQuery<TResult,TKey>> Query<TResult,TKey>(Func<T, IQueryable<TResult>> query, Expression<Func<TResult,TKey>> keySelector)
        {
            return await Task.Run(() =>
            {
                using (var context = _contextFactory())
                {
                    var queryable = query(context);
                    var mapperFunc = BuildMapperFunc(context, queryable);

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

                    var userQuery = new TypedUserQuery<TResult, TKey>(mapperFunc, keySelector.Compile(), text,
                        primaries.First(), primaries.Skip(1).ToArray());
                    userQuery.SetCustomQuery(_db.CustomQuery(userQuery));
                    return userQuery;
                }
            });
        }

        /// <summary>
        /// Extracts the function that Linq2Sql use to map the initial result
        /// </summary>
        /// <typeparam name="TResult">Type of the rows</typeparam>
        /// <param name="context">The linq context of type <typeparamref name="T"/></param>
        /// <param name="queryable">The Queryable that already has all Expressions applied</param>
        /// <returns>The function that maps the result to the reader</returns>
        private Func<DbDataReader, IEnumerator> BuildMapperFunc<TResult>(T context, IQueryable<TResult> queryable)
        {
            var providerProperty = typeof(T).GetProperty("Provider", BindingFlags.Instance | BindingFlags.NonPublic);
            var sqlProvider = providerProperty.GetValue(context) as SqlProvider;
            var methods = sqlProvider.GetType().GetMethods(BindingFlags.Instance | BindingFlags.NonPublic);

            var compileMethod = methods.First(m => m.Name == "System.Data.Linq.Provider.IProvider.Compile");
            var compiledQuery = compileMethod.Invoke(sqlProvider, new object[] {queryable.Expression});

            var factoryField = compiledQuery.GetType().GetField("factory", BindingFlags.Instance | BindingFlags.NonPublic);
            var subQueriesField = compiledQuery.GetType().GetField("subQueries", BindingFlags.Instance | BindingFlags.NonPublic);
            var factory = factoryField.GetValue(compiledQuery);
            var subQueries = subQueriesField.GetValue(compiledQuery);

            var createMethod = factory.GetType().GetMethod("Create");

            var reader = Expression.Parameter(typeof(DbDataReader));
            var body = Expression.Call
            (
                Expression.Constant(factory, factory.GetType()),
                createMethod,
                reader,
                Expression.Constant(false),
                Expression.Constant(sqlProvider),
                Expression.Constant(null, typeof(object[])),
                Expression.Constant(null, typeof(object[])),
                Expression.Constant(subQueries)
            );
            var mapperFunc = Expression.Lambda<Func<DbDataReader, IEnumerator>>(body, reader).Compile();
            return mapperFunc;
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
            if (DisposeBehavior == DisposeBehavior.CleanupSchema)
            {
                _db?.CleanupSchemaChanges();
            }
            else
            {
                _db?.Dispose();
            }
        }
    }
}

