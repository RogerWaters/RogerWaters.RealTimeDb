using System;
using System.Collections.Generic;
using System.Data.Linq;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RogerWaters.RealTimeDb.Configuration;

namespace RogerWaters.RealTimeDb.Linq2Sql
{
    /// <summary>
    /// Builder to configure database for use with Linq
    /// </summary>
    /// <typeparam name="T">Type of the <see cref="DataContext"/></typeparam>
    public class RealtimeDbDataContextBuilder<T> : DatabaseConfigBuilder where T: DataContext
    {
        /// <summary>
        /// The factory to create new context instances
        /// </summary>
        public Func<T> ContextFactory { get; }

        /// <summary>
        /// Initialize a new instance of <see cref="RealtimeDbDataContextBuilder{T}"/>
        /// </summary>
        /// <param name="contextFactory"> The factory to create new context instances</param>
        public RealtimeDbDataContextBuilder(Func<T> contextFactory) : base(contextFactory().Connection.ConnectionString)
        {
            ContextFactory = contextFactory;
        }

        /// <summary>
        /// Build the <see cref="RealtimeDbDataContext{T}"/> from the current configured state
        /// </summary>
        /// <returns>The configured instance</returns>
        public new RealtimeDbDataContext<T> Build()
        {
            return new RealtimeDbDataContext<T>(this);
        }
    }
}
