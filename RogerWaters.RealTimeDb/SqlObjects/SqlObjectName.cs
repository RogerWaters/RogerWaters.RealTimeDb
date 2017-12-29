using System;

namespace RogerWaters.RealTimeDb.SqlObjects
{
    /// <summary>
    /// Represents the name of an Sql-Object that is defined by schema and name
    /// </summary>
    public sealed class SqlObjectName : IEquatable<SqlObjectName>
    {
        /// <summary>
        /// The name of the object without schema
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// The schema of the object
        /// </summary>
        public string Schema { get; }

        /// <summary>
        /// Initialize a new instance of <see cref="SqlObjectName"/>
        /// </summary>
        /// <param name="name">The name of the object without schema</param>
        /// <param name="schema">The schema of the object</param>
        public SqlObjectName(string name, string schema = null)
        {
            const string defaultSchema = "dbo";

            Schema = schema ?? defaultSchema;
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Name = name.Trim('[', ']');
            Schema = Schema.Trim('[', ']');
        }
        
        /// <summary>
        /// Parse the table name. Allowed formats:<br/>
        /// - [schema].[name]<br/>
        /// - schema.name<br/>
        /// - [name]<br/>
        /// - name<br/>
        /// </summary>
        /// <param name="objectText">the text to parse</param>
        /// <returns>the parsed <see cref="SqlObjectName"/></returns>
        public static SqlObjectName Parse(string objectText)
        {
            var parts = (objectText ?? string.Empty).Split(new[] { "." }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length > 0)
            {
                if (parts.Length > 1)
                {
                    if (parts.Length > 2)
                    {
                        throw new ArgumentException("TableName is invalid. Expected name oder schema.name", nameof(objectText));
                    }
                    return new SqlObjectName(parts[1], parts[0]);
                }
                return new SqlObjectName(parts[0]);
            }
            throw new ArgumentException("TableName is invalid", nameof(objectText));
        }

        /// <summary>
        /// Parse the <see cref="SqlObjectName"/> from string
        /// </summary>
        /// <param name="value">The string that will be converted to <see cref="SqlObjectName"/></param>
        public static implicit operator SqlObjectName(string value)
        {
            return Parse(value);
        }

        /// <summary>
        /// Convert the <paramref name="value"/> to it's string representation
        /// </summary>
        /// <param name="value"></param>
        public static implicit operator string(SqlObjectName value)
        {
            return value.ToString();
        }

        /// <summary>
        /// Format the current instance as [{Schema}].[{Name}]
        /// </summary>
        /// <returns>The formatted string</returns>
        public override string ToString()
        {
            return $"[{Schema}].[{Name}]";
        }

        /// <inheritdoc />
        public bool Equals(SqlObjectName other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Name, other.Name) && string.Equals(Schema, other.Schema);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj is SqlObjectName name && Equals(name);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name != null ? Name.GetHashCode() : 0) * 397) ^ (Schema != null ? Schema.GetHashCode() : 0);
            }
        }

        /// <summary>
        /// Abbreviation for <see cref="Equals(SqlObjectName)"/>
        /// </summary>
        /// <param name="left">The left part of comparison</param>
        /// <param name="right">The right part of comparison</param>
        /// <returns>true if both sides are equal</returns>
        public static bool operator ==(SqlObjectName left, SqlObjectName right)
        {
            return Equals(left, right);
        }
        
        /// <summary>
        /// Abbreviation for <see cref="Equals(SqlObjectName)"/> == false
        /// </summary>
        /// <param name="left">The left part of comparison</param>
        /// <param name="right">The right part of comparison</param>
        /// <returns>true if both sides are not equal</returns>
        public static bool operator !=(SqlObjectName left, SqlObjectName right)
        {
            return !Equals(left, right);
        }
    }
}
