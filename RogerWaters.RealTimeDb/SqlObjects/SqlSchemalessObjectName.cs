using System;

namespace RogerWaters.RealTimeDb.SqlObjects
{
    /// <summary>
    /// Represents an Sql-Object that is not schema qualified
    /// </summary>
    public sealed class SqlSchemalessObjectName:IEquatable<SqlSchemalessObjectName>
    {
        /// <summary>
        /// The name of the Sql-Object
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Initialize a new instance of <see cref="SqlSchemalessObjectName"/>
        /// </summary>
        /// <param name="name">The name of the object</param>
        public SqlSchemalessObjectName(string name)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Name = name.Trim('[', ']');
        }

        /// <summary>
        /// Create a new <see cref="SqlSchemalessObjectName"/> from <paramref name="value"/>
        /// </summary>
        /// <param name="value">The name of the <see cref="SqlSchemalessObjectName"/></param>
        public static implicit operator SqlSchemalessObjectName(string value)
        {
            return new SqlSchemalessObjectName(value);
        }

        /// <summary>
        /// returns the Sql-Escaped representation of <paramref name="value"/>
        /// </summary>
        /// <param name="value">The <see cref="SqlSchemalessObjectName"/></param>
        public static implicit operator string(SqlSchemalessObjectName value)
        {
            return value.ToString();
        }

        /// <summary>
        /// Escape the name and format the instance
        /// </summary>
        /// <returns>The formatted name</returns>
        public override string ToString()
        {
            return $"[{Name}]";
        }

        /// <inheritdoc />
        public bool Equals(SqlSchemalessObjectName other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Name, other.Name);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj is SqlSchemalessObjectName name && Equals(name);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                return (Name != null ? Name.GetHashCode() : 0) * 397;
            }
        }
        
        /// <summary>
        /// Abbreviation for <see cref="Equals(SqlSchemalessObjectName)"/>
        /// </summary>
        /// <param name="left">The left part of comparison</param>
        /// <param name="right">The right part of comparison</param>
        /// <returns>true if both sides are equal</returns>
        public static bool operator ==(SqlSchemalessObjectName left, SqlSchemalessObjectName right)
        {
            return Equals(left, right);
        }
        
        /// <summary>
        /// Abbreviation for <see cref="Equals(SqlSchemalessObjectName)"/> == false
        /// </summary>
        /// <param name="left">The left part of comparison</param>
        /// <param name="right">The right part of comparison</param>
        /// <returns>true if both sides are not equal</returns>
        public static bool operator !=(SqlSchemalessObjectName left, SqlSchemalessObjectName right)
        {
            return !Equals(left, right);
        }
    }
}
