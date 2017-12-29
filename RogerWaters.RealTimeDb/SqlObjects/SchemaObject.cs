namespace RogerWaters.RealTimeDb.SqlObjects
{
    /// <summary>
    /// Represents a Sql-Server schema-bound object that alters the schema to be usable
    /// </summary>
    public abstract class SchemaObject
    {
        /// <summary>
        /// Remove all the changes from schema
        /// </summary>
        /// <remarks>
        /// This should be called right after dispose
        /// </remarks>
        public abstract void CleanupSchemaChanges();
    }
}
