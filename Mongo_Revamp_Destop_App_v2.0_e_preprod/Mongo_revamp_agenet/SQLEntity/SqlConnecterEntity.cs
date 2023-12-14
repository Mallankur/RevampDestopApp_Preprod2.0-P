using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mongo_revamp_agenet.SQLEntity
{
    public  class SqlConnecterEntity
    {
       
        public int? Sapunitno { get; set; }
        public long? Cycloeno { get; set; }
        public long? CycleID { get; set; }
        public int? DOS { get; set; }
        public long? DOSDATE { get; set; }
        public int? CC_Fields_Defs_Id { get; set; }

        public decimal? CSISValue { get; set; }

        public decimal? ImputedValue { get; set; }

        public decimal? ImputedValueMetric { get; set; }

        public decimal? ImputedValueImperial { get; set; }

        public decimal? CleansedValue { get; set; }

        public decimal? ValueMetric { get; set; }

        public decimal? ImportedValue { get; set; }


        public long? CSISDataTypeId { get; set; }

        public long? CSISTestRunId { get; set; }

        public long? CSISPredictionId { get; set; }

        public long? EOMobileLabId { get; set; }

        public long? ReportDataEntityId { get; set; }

        public bool? IgnoreError { get; set; }

        public long ApplicationId { get; set; }

        public int? Mode { get; set; }

    }
}
