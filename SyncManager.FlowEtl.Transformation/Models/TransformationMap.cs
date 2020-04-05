using System;
using System.Xml.Serialization;

namespace SyncManager.FlowEtl.Transformation.Models
{
    public class TransformationMap
    {
        [XmlAttribute("MappingType")]
        public MappingType MappingType { get; set; }

        [XmlAttribute("Output")]
        public string Output { get; set; }

        [XmlAttribute("OutputType")]
        public ValueType OutputType { get; set; }

        [XmlAttribute("Expression")]
        public string Expression { get; set; }

        [XmlAttribute("KeyExpression")]
        public string KeyExpression { get; set; }

        public bool IsRequired { get; set; }

        [XmlElement("DataMapping")]
        public DataMapping DataMapping { get; set; }

        [XmlElement("Description")]
        public string Description { get; set; }

        [XmlElement("TransformIfColumnNotExist")]
        public bool TransformOnlyIfColumnExists { get; set; }

        public bool IsNotFilled()
        {
            return string.IsNullOrEmpty(Expression) && string.IsNullOrEmpty(KeyExpression) &&
                   DataMapping == null;
        }
    }
}