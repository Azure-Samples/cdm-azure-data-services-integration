// <copyright file = "Parse.cs" company="Microsoft">
// Copyright (c) Microsoft. All rights reserved.
// </copyright>

using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.CdmFolders.ObjectModel;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;

namespace cdmtosqldw
{
    public static class Parse
    {
        [FunctionName("Parse")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)]HttpRequestMessage req, TraceWriter log)
        {
            string requestContent = await req.Content.ReadAsStringAsync();
            JObject payload = JObject.Parse(requestContent);

            string modelJSON = payload["model"].ToString();
            JObject dataTypeMap = payload["dataTypeMap"] as JObject;

            JArray results = new JArray();
            Model model = new Model();
            try
            {
                model.FromJson(modelJSON);
                model.ValidateModel();

                foreach (LocalEntity entity in model.Entities)
                {
                    JObject entityJObject = new JObject();

                    string createTableQuery = "Create Table {0} ({1})";
                    StringBuilder columns = new StringBuilder();
                    string tableName = string.Format("[{0}]", entity.Name.Replace(" ", string.Empty));
                    
                    string modifiedTimeJPath = string.Format("$..entities[?(@.name == '{0}')].modifiedTime", entity.Name);
                    IsoDateTimeConverter dateTimeConvertor = new IsoDateTimeConverter()
                    {
                        DateTimeStyles = System.Globalization.DateTimeStyles.AdjustToUniversal,
                    };

                    string entityModifiedTime = payload.SelectToken(modifiedTimeJPath)?.ToString(Formatting.None, dateTimeConvertor).Replace("\"", "");

                    JArray tableStructure = new JArray();
                    foreach (var attr in entity.Attributes)
                    {
                        string columnType = attr.DataType.ToString();
                        JToken sqlDwTypeToken = null;
                        if(!dataTypeMap.TryGetValue(columnType, StringComparison.OrdinalIgnoreCase, out sqlDwTypeToken))
                        {
                            throw new Exception($"No type mapping found for {columnType}");
                        }
                        string sqlDwType = sqlDwTypeToken.ToString();
                        string column = string.Format("[{0}] {1},", attr.Name, sqlDwType);
                        columns.Append(column);

                        JObject destColumnStructure = new JObject();
                        destColumnStructure.Add("name", attr.Name);
                        destColumnStructure.Add("type", columnType);
                        tableStructure.Add(destColumnStructure);
                    }

                    string columnsQuery = columns.ToString();
                    createTableQuery = string.Format(createTableQuery, tableName, columnsQuery.TrimEnd(','));

                    JArray dataFileLocationsArray = new JArray();
                    foreach (var partition in entity.Partitions)
                    {
                        string relativePath = HttpUtility.UrlDecode(partition.Location.AbsolutePath);
                        string folderPath = relativePath.Substring(1, relativePath.LastIndexOf('/'));
                        string filePath = relativePath.Substring(relativePath.LastIndexOf('/') + 1);

                        JObject datafileLocation = new JObject();
                        datafileLocation.Add("folderPath", folderPath);
                        datafileLocation.Add("filePath", filePath);
                        datafileLocation.Add("refreshTime", partition.RefreshTime.Value.DateTime.ToUniversalTime());
                        dataFileLocationsArray.Add(datafileLocation);
                    }

                    entityJObject.Add("name", entity.Name);
                    entityJObject.Add("tableName", tableName);
                    entityJObject.Add("modifiedTime", entityModifiedTime);
                    entityJObject.Add("tableStructure", tableStructure);
                    entityJObject.Add("query", createTableQuery);
                    entityJObject.Add("datafileLocations", dataFileLocationsArray);

                    results.Add(entityJObject);
                }

                JObject response = new JObject();
                response.Add("result", results);

                return req.CreateResponse(HttpStatusCode.OK, response);
            }
            catch (Exception e)
            {
                HttpResponseMessage responseMessage = req.CreateResponse(HttpStatusCode.BadRequest, string.Format("Failed to Parse Model. Error {0}", e.Message));
                return responseMessage;
            }
        }
    }
}
