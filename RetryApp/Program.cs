using System;
using System.Collections.Generic;
using System.IO;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Newtonsoft.Json.Linq;

namespace RetryApp
{
    class Program
    {
        private static string jsonFile;

        static void Main(string[] args)
        {
            GetFilePath();

            RePublishMessages();

            Console.ReadLine();
        }

        private static void RePublishMessages()
        {
            var json = File.ReadAllText(jsonFile);
            List<PubsubMessage> unPublishedMessages = new List<PubsubMessage>();

            try
            {
                var jsonObj = JObject.Parse(json);
                var messageArray = jsonObj.GetValue("messages") as JArray;

                if (messageArray != null)
                {
                    foreach (var newMessage in messageArray)
                    {
                        PubsubMessage pubsubMessage = new PubsubMessage
                        {
                            Data = ByteString.CopyFromUtf8(newMessage["Data"].ToString()),
                            Attributes =
                        {
                            {"description",newMessage["Attributes"]["description"].ToString() }
                        }
                        };

                        //Now we can call Google Publish Method Again
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Add Error : " + ex.Message.ToString());
            }
        }

        private static void GetFilePath()
        {
            string path = Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName;
            //for mac or linux OS
            jsonFile = path.Substring(0, path.IndexOf("/bin")) + "/pending_messages.json";

            //for windows OS
            //jsonFile = path.Substring(0, path.IndexOf("\\bin")) + "\\pending_messages.json";
        }

    }
}
