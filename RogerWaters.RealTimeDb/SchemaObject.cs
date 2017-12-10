using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RogerWaters.RealTimeDb
{
    public abstract class SchemaObject
    {
        public abstract void CleanupSchemaChanges();
    }
}
