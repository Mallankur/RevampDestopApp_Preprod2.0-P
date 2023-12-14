using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver;
using System.Security.Cryptography.X509Certificates;
using ThirdParty.Json.LitJson;
using Mongo_revamp_agenet.MongoEntity;
using Serilog;
using System.Diagnostics;

using MongoDB.Driver.Core.Configuration;
using MongoDB.Bson.Serialization;
using Mongo_revamp_agenet.MongoDamKohler;

namespace Mongo_revamp_agenet
{
    public class Program
    {
        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
        .WriteTo.Console()
        .WriteTo.File(
            @"C:\Users\ankur\Desktop\AvinexAnkurAPP\Catcheck_Revamp_APP_Log\log.txt",
            rollingInterval: RollingInterval.Day,
            outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] {Message:lj}{NewLine}{Exception}",
            fileSizeLimitBytes: null,
            retainedFileCountLimit: null,
            shared: true,
            flushToDiskInterval: TimeSpan.FromSeconds(1))
                .CreateLogger();

            var fillDataStopwatch = Stopwatch.StartNew();

            var connectionstring = "server=AVINEXSERVER6;database=CatCheckPro;Integrated Security=false;User ID=CCAdmin;Password=Catalyst1*;Trusted_Connection=No;";

           //var res = MongoBatchOperation.RevampSQLAgent(connectionstring, "CC_Revamp_uspGetUMData", 1289, ""); //
            var res3081 = SqlFourierConvectionAdpater.RevampSQLFourierAgent(connectionstring, "CC_Revamp_uspGetUMData", 3081, ""); 

            string mongoConnectionString = "mongodb://10.2.10.19:27017/";
            string mongoDatabaseName = "UnitMonitringCyceleData";
            string mongoCollectionName = "CycleData3081";

            MongoClient mongoClient = new MongoClient(mongoConnectionString);
            IMongoDatabase mongoDatabase = mongoClient.GetDatabase(mongoDatabaseName);
            IMongoCollection<MongoRevampEntity> mongoCollection = mongoDatabase.GetCollection<MongoRevampEntity>(mongoCollectionName);

            var pushTOMongoStopwatch = Stopwatch.StartNew();
            int batchCount = 0;

            foreach (var jsonData in res3081)
            {
               // var batchDocuments = Newtonsoft.Json.JsonConvert.DeserializeObject<IEnumerable<MongoRevampEntity>>(jsonData, new DateTimeToMillisecondsConverter());

                var batchDocuments = Newtonsoft.Json.JsonConvert.DeserializeObject<MongoRevampEntity>(jsonData, new DateTimeToMillisecondsConverter());


                var batchInsertStopwatch = Stopwatch.StartNew();
               // mongoCollection.InsertManyAsync(batchDocuments);
               mongoCollection.InsertOneAsync(batchDocuments);  
                batchInsertStopwatch.Stop();

                batchCount++;

               // Log.Information("Batch {BatchNumber} insert took: {ElapsedMilliseconds} ms", batchCount, batchInsertStopwatch.ElapsedMilliseconds);
            }

            pushTOMongoStopwatch.Stop();

            Log.Information("Total data fill operation took: {ElapsedMilliseconds} ms", fillDataStopwatch.ElapsedMilliseconds);
            Log.Information("Total data push to MongoDB operation took: {ElapsedMilliseconds} ms", pushTOMongoStopwatch.ElapsedMilliseconds);

            Log.CloseAndFlush();
            Console.ReadLine();
        }
    }
    }

            
        








            
             




            
        
    


