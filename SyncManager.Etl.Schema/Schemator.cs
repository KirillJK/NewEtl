using System;
using System.Collections.Generic;
using System.Globalization;
using Dynamitey;
using ImpromptuInterface;
using SyncManager.Etl.Common;
using ValueType = SyncManager.Common.ValueType;

namespace SyncManager.Etl.Schema
{
    public interface ISchemator
    {
        void Schema(List<DataSourceSchemaItem> schema, SourceContext sourceContext);
    }


    public class Schemator: ISchemator
    {
        private readonly Lazy<ICastProvider> _castProvider;

        public Schemator()
        {
            _castProvider = new Lazy<ICastProvider>(() => DefaultCastProvider.Instance);
        }

        public void Schema(List<DataSourceSchemaItem> schema, SourceContext sourceContext)
        {
            foreach (var dataSourceSchemaItem in schema)
            {
                var columnValue = sourceContext.Source[dataSourceSchemaItem.ColumnName];
                try
                {
                    sourceContext.Source.Remove(dataSourceSchemaItem.ColumnName);
                    if (CheckMandatory(dataSourceSchemaItem, sourceContext))
                    {
                        sourceContext.Source[dataSourceSchemaItem.GetName()] = GetSourceValue(columnValue, dataSourceSchemaItem.Type, dataSourceSchemaItem.IsRequired);
                    }
                    else
                    {
                        sourceContext.AddErrorForColumn(dataSourceSchemaItem.ColumnName, "Value for required field is missed", SourceContext.ErrorTypeSchema);
                    }
                }
                catch (Exception e)
                {
                    sourceContext.AddErrorForColumn(dataSourceSchemaItem.GetName(), e, SourceContext.ErrorTypeSchema);
                }
            }
        }

        private bool CheckMandatory(DataSourceSchemaItem item, SourceContext context)
        {
            if (!item.IsRequired) return true;
            if (!context.Source.ContainsKey(item.ColumnName) || context.Source[item.ColumnName] == null ||
                context.Source[item.ColumnName].ToString() == "") return false;
            return true;
        }

        private object GetSourceValue(object columnValue, ValueType? outputType, bool isRequired)
        {

            if (string.IsNullOrWhiteSpace(columnValue?.ToString()))
            {
                return null;
            }

            var doCast = outputType.HasValue && (isRequired || !string.IsNullOrWhiteSpace(columnValue.ToString()));

            if (doCast)
            {
                columnValue = CastSchemaItemType(columnValue, outputType.Value);
            }

            return columnValue;
        }

        private dynamic CastSchemaItemType(object arg, ValueType itemType)
        {
            if ((arg is string) && ((itemType == ValueType.Int) || itemType == ValueType.Decimal || itemType == ValueType.Long || itemType == ValueType.Double))
            {
                if (arg.ToString().StartsWith("(") && (arg.ToString().EndsWith(")")))
                {
                    arg = arg.ToString().Replace("(", "-").Replace(")", "");
                }
            }
            var type = ConvertValueType(itemType);
            if (type == null)
            {
                return arg;
            }
            var argType = arg.GetType();
            if (argType == type)
            {
                return arg;
            }
           
            var cast = _castProvider.Value.GetCast(argType, type);
            return cast != null
                ? cast(arg)
                : Impromptu.InvokeConvert(arg, type, true);
        }

        private static Type ConvertValueType(ValueType itemType)
        {
            if (SupportedTypes.TryGetValue(itemType, out var result))
                return result;
            throw new ArgumentOutOfRangeException(nameof(itemType), itemType, "Unsupported type");
        }

        private static readonly IDictionary<ValueType, Type> SupportedTypes = new Dictionary<ValueType, Type>
        {
            [ValueType.Guid] = typeof(Guid),
            [ValueType.DateTime] = typeof(DateTime),
            [ValueType.Custom] = null,
            [ValueType.Object] = null,
            [ValueType.Boolean] = typeof(bool),
            [ValueType.String] = typeof(string),
            [ValueType.Int] = typeof(int),
            [ValueType.Decimal] = typeof(decimal),
            [ValueType.Long] = typeof(long),
            [ValueType.Double] = typeof(double)
        };



    }



    public class DefaultCastProvider : ICastProvider
    {
        private static IList<Tuple<Type, Type, Func<object, object>>> wellKnownCasts =
            new List<Tuple<Type, Type, Func<object, object>>>
        {
            new Tuple<Type, Type, Func<object, object>>(typeof(object), typeof(string),
                arg => (string)arg),
            new Tuple<Type, Type, Func<object, object>>(typeof(string), typeof(bool),
                arg => bool.Parse((string)arg)),
            new Tuple<Type, Type, Func<object, object>>(typeof(string), typeof(int),
                arg => int.Parse((string)arg)),
            new Tuple<Type, Type, Func<object, object>>(typeof(string), typeof(long),
                arg => long.Parse((string)arg)),
            new Tuple<Type, Type, Func<object, object>>(typeof(string), typeof(decimal),
                arg => decimal.Parse((string)arg, NumberStyles.Any)),
            new Tuple<Type, Type, Func<object, object>>(typeof(string), typeof(double),
                arg => double.Parse((string)arg, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture)),
            new Tuple<Type, Type, Func<object, object>>(typeof(string), typeof(Guid),
                arg => new Guid((string)arg)),
            new Tuple<Type, Type, Func<object, object>>(typeof(string), typeof(DateTime),
                arg => DateTime.Parse((string)arg)),
            new Tuple<Type, Type, Func<object, object>>(typeof(string), typeof(byte[]),
                arg => Convert.FromBase64String ((string)arg)),
        };

        public static readonly ICastProvider Instance = new DefaultCastProvider();

        public Func<object, object> GetCast(Type one, Type two)
        {
            foreach (var cast in wellKnownCasts)
            {
                if (cast.Item1.IsAssignableFrom(one) && two == cast.Item2)
                    return cast.Item3;
            }
            return null;
        }
    }
    public interface ICastProvider
    {
        Func<object, object> GetCast(Type one, Type two);
    }


}