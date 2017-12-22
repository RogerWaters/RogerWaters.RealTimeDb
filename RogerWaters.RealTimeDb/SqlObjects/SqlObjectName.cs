using System;

namespace RogerWaters.RealTimeDb.SqlObjects
{
    public sealed class SqlObjectName:IEquatable<SqlObjectName>
    {
        public string Name { get; }
        public string Schema { get; }

        public SqlObjectName(string name, string schema = null)
        {
            Schema = schema ?? "dbo";
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

        public static implicit operator SqlObjectName(string value)
        {
            return Parse(value);
        }
        public static implicit operator string(SqlObjectName value)
        {
            return value.ToString();
        }

        public override string ToString()
        {
            return $"[{Schema}].[{Name}]";
        }

        public bool Equals(SqlObjectName other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Name, other.Name) && string.Equals(Schema, other.Schema);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj is SqlObjectName name && Equals(name);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name != null ? Name.GetHashCode() : 0) * 397) ^ (Schema != null ? Schema.GetHashCode() : 0);
            }
        }

        public static bool operator ==(SqlObjectName left, SqlObjectName right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(SqlObjectName left, SqlObjectName right)
        {
            return !Equals(left, right);
        }
    }
}
