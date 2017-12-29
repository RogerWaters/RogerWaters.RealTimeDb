using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RogerWaters.RealTimeDb.SqlObjects.Queries
{
    public abstract class QueryObserver : IDisposable
    {
        /// <summary>
        /// Unique identifier for query
        /// </summary>
        public Guid Guid { get; }
        
        protected QueryObserver(Guid guid)
        {
            Guid = guid;
        }
        
        public abstract void Dispose();
    }
}
