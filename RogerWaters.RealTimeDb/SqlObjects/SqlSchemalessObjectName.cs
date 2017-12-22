using System;

namespace RogerWaters.RealTimeDb.SqlObjects
{
    public sealed class SqlSchemalessObjectName:IEquatable<SqlSchemalessObjectName>
    {
        public string Name { get; }

        public SqlSchemalessObjectName(string name)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Name = name.Trim('[', ']');
        }
        

        public static implicit operator SqlSchemalessObjectName(string value)
        {
            return new SqlSchemalessObjectName(value);
        }
        public static implicit operator string(SqlSchemalessObjectName value)
        {
            return value.ToString();
        }

        public override string ToString()
        {
            return $"[{Name}]";
        }

        public bool Equals(SqlSchemalessObjectName other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Name, other.Name);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj is SqlSchemalessObjectName name && Equals(name);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Name != null ? Name.GetHashCode() : 0) * 397;
            }
        }

        public static bool operator ==(SqlSchemalessObjectName left, SqlSchemalessObjectName right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(SqlSchemalessObjectName left, SqlSchemalessObjectName right)
        {
            return !Equals(left, right);
        }
    }
}
