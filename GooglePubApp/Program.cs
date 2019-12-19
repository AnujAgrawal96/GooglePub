using System;
using System.Collections.Generic;
using System.IO;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GooglePubApp
{
    class Program
    {
        private static string jsonFile;

        static void Main(string[] args)
        {
            GetFilePath();

            Console.WriteLine("Google Pub Demo :- ");
            Console.WriteLine("--------------------");

            // Instantiates client and creates topic
            PublisherServiceApiClient publisher;
            TopicName topicName;
            InstantiateClient(out publisher, out topicName);

            Console.WriteLine("----------------------");
            Console.WriteLine("Publishing messages to the topic");

            List<PubsubMessage> unPublishedMessages = PublishMessages(publisher, topicName);

            //Adding messages to Json File
            if (unPublishedMessages.Count > 0)
            {
                AddMessagesToJSONfile(unPublishedMessages);
            }

            Console.WriteLine("----------Done!!-----------");
            Console.ReadLine();
        }

        private static void AddMessagesToJSONfile(List<PubsubMessage> unPublishedMessages)
        {
            try
            {
                var json = File.ReadAllText(jsonFile);
                var jsonObj = JObject.Parse(json);
                var messageArray = jsonObj.GetValue("messages") as JArray;

                foreach (PubsubMessage message in unPublishedMessages)
                {
                    var newMessage = JObject.Parse(JsonConvert.SerializeObject(message));
                    messageArray.Add(newMessage);
                }

                jsonObj["messages"] = messageArray;
                string newJsonResult = JsonConvert.SerializeObject(jsonObj, Formatting.Indented);
                File.WriteAllText(jsonFile, newJsonResult);
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

        private static List<PubsubMessage> PublishMessages(PublisherServiceApiClient publisher, TopicName topicName)
        {
            List<PubsubMessage> unPublishedMessages = new List<PubsubMessage>();

            //publish message to the topic

            for (int i = 0; i < 2; i++)
            {
                try
                {
                    PubsubMessage pubsubMessage = new PubsubMessage
                    {
                        Data = ByteString.CopyFromUtf8(i.ToString()),
                        Attributes =
                        {
                            {"description","Demo message" }
                        }
                    };
                    System.Threading.Thread.Sleep(1000);
                    publisher.Publish(topicName, new[] { pubsubMessage });
                    Console.WriteLine("Published Message : " + i.ToString());
                }
                catch (Grpc.Core.RpcException e)
                when (e.Status.StatusCode == Grpc.Core.StatusCode.Unavailable)
                {
                    Console.WriteLine("Some services are unavailable!! Will retry after some time.");
                    unPublishedMessages.Add(new PubsubMessage()
                    {
                        Data = ByteString.CopyFromUtf8(i.ToString()),
                        Attributes =
                        {
                            {"description", "Demo message" }
                        }
                    });
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Add Error : " + ex.Message.ToString());
                }
            }

            return unPublishedMessages;
        }

        private static void InstantiateClient(out PublisherServiceApiClient publisher, out TopicName topicName)
        {
            publisher = PublisherServiceApiClient.Create();

            //your Google Cloud Platform project id
            string projectId = "YOUR-PROJECT-ID";

            //The name of topic
            topicName = new TopicName(projectId, "my-topic");

            //creates a new topic
            Console.WriteLine("step-1 : creating Topic-");
            try
            {
                Topic topic = publisher.CreateTopic(topicName);
                Console.WriteLine($"Topic {topic.Name} created.");
            }
            catch (Grpc.Core.RpcException e)
            when (e.Status.StatusCode == Grpc.Core.StatusCode.AlreadyExists)
            {
                Console.WriteLine($"Topic {topicName} already exists.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Add Error : " + ex.Message.ToString());
            }
        }
    }
}
