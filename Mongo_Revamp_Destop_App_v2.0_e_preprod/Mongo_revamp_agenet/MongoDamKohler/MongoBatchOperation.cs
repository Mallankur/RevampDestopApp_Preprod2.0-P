using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mongo_revamp_agenet
{
    public class MongoBatchOperation
    {/// <summary>
    /// AnkurMallDesigan operationAlogoritham.
    /// xa = 1 - e^-t/tu
    /// xa = da/(DA+1)
    /// </summary>
    /// <param name="sqlConnectionString"></param>
    /// <param name="storeProcedureName"></param>
    /// <param name="CycleId"></param>
    /// <param name="rdids"></param>
    /// <returns></returns>
    /// 
       
	        
		
	
	
        public static IEnumerable<string> RevampSQLAgent(string sqlConnectionString, string storeProcedureName, int CycleId, string rdids)
        {




            const int batchSize = 100000;

            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();

                SqlCommand sqlCommand = new SqlCommand(storeProcedureName, sqlConnection);
                sqlCommand.CommandType = CommandType.StoredProcedure;
                sqlCommand.Parameters.AddWithValue("@CycleId", CycleId);
                sqlCommand.Parameters.AddWithValue("@RDIds", rdids);

                SqlDataAdapter adapter = new SqlDataAdapter(sqlCommand);
                DataTable dataTable = new DataTable();
                adapter.Fill(dataTable);  //  this line throw the run time excption for memeory out of excption 

                // and pleses also update the code eariiler i have exessed the data with limit 78 lakh 


                int totalRows = dataTable.Rows.Count; // 22
                int batches = (int)Math.Ceiling((double)totalRows / batchSize);

                for (int i = 0; i < batches; i++)
                {
                    int startIndex = i * batchSize;
                    int endIndex = Math.Min((i + 1) * batchSize, totalRows);

                    DataTable batchTable = dataTable.Clone();
                    for (int j = startIndex; j < endIndex; j++)
                    {
                        batchTable.ImportRow(dataTable.Rows[j]);
                    }



                    string jsonData = Newtonsoft.Json.JsonConvert.SerializeObject(batchTable);


                    yield return jsonData;
                }
            }

        }
    }

    
}
