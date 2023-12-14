using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;

namespace Mongo_revamp_agenet.MongoDamKohler
{
    public class SqlFourierConvectionAdpater
    {
        /// <summary>
        /// AnkurAlogrithumDev
        /// </summary>
        /// <param name="sqlConnectionString"></param>
        /// <param name="storeProcedureName"></param>
        /// <param name="CycleId"></param>
        /// <param name="rdids"></param>
        /// <returns></returns>
        public static IEnumerable<string> RevampSQLFourierAgent(string sqlConnectionString, string storeProcedureName, int CycleId, string rdids)
        {
            const int batchSize = 100000;

            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();

                SqlCommand sqlCommand = new SqlCommand(storeProcedureName, sqlConnection);
                sqlCommand.CommandType = CommandType.StoredProcedure;
                sqlCommand.Parameters.AddWithValue("@CycleId", CycleId);
                sqlCommand.Parameters.AddWithValue("@RDIds", rdids);

                using (SqlDataReader reader = sqlCommand.ExecuteReader())
                {
                    int ordinal = 0;

                  
                    var columnNames = Enumerable.Range(0, reader.FieldCount)
                        .Select(reader.GetName)
                        .ToList();

                    while (reader.Read())
                    {
                        var rowData = new object[reader.FieldCount];
                        reader.GetValues(rowData);

                        var rowDataDict = new Dictionary<string, object>();
                        for (int i = 0; i < rowData.Length; i++)
                        {
                            rowDataDict.Add(columnNames[i], rowData[i]);
                        }

                        string jsonData = Newtonsoft.Json.JsonConvert.SerializeObject(rowDataDict);
                        yield return jsonData;
                    }
                }
            }
        }

    }
}
