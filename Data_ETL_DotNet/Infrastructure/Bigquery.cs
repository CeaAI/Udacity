using System;
using Google.Cloud.BigQuery.V2;
namespace Infrastructure
{
    public class BigQuery
    {
      private readonly BigQueryClient  _bqClient;
      private const string projectId = "river-yew-256601";

      public BigQuery(){
            _bqClient = BigQueryClient.Create(projectId);
      }
    }
}