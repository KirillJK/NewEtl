using System.Xml.Serialization;

namespace SyncManager.FlowEtl.Cleanup
{
    public class CleanupRule
    {
        //[XmlElement("ColumnIndex")]
        //public int? ColumnIndex { get; set; }
        [XmlAttribute("ColumnName")]
        public string ColumnName { get; set; }
        [XmlAttribute("Condition")]
        public CleanupCondition Condition { get; set; }
        [XmlAttribute("ConditionalArgument")]
        public string ConditionArgument { get; set; }
        [XmlAttribute("Action")]
        public CleanupAction Action { get; set; }
        //[XmlAttribute("Value")]
        //public string Value { get; set; }
        [XmlAttribute("Expression")]
        public string Expression { get; set; }
        [XmlAttribute]
        public bool IsEnabled { get; set; }
    }
}
