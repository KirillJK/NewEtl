using System;
using System.Xml.Serialization;

namespace SyncManager.Etl.Schema
{
    public class DataSourceSchemaItem
    {
        [XmlAttribute("Index")]
        public int Index { get; set; }
        [XmlAttribute("ColumnName")]
        public string ColumnName { get; set; }
        [XmlAttribute("Alias")]
        public string Alias { get; set; }
        [XmlAttribute("IsRequired")]
        public bool IsRequired { get; set; }
        [XmlAttribute("FullType")]
        public string FullType { get; set; }
        [XmlElement("Type")]
        public SyncManager.Common.ValueType? Type { get; set; }
        [XmlElement("Description")]
        public string Description { get; set; }
        public bool IsNotExistInFile { get; set; }
        public string GetName()
        {
            if (string.IsNullOrEmpty(Alias)) return ColumnName;
            return Alias;
        }
    }
}
