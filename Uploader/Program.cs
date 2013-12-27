using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.ServiceBus;
using Uploader.SeviceBusMessaging;
using System.Text.RegularExpressions;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System.IO;
namespace Uploader
{
    class Program
    {
        static CloudStorageAccount s_Account = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("Uploader.Storage.ConnectionString"));
        static CloudBlobContainer s_Container;
        static QueueClient s_QueueClient;
        static void Main(string[] args)
        {
            if(args.Length != 3)
            {
                PrintUsage();
            }

            try
            {
                
                //Get Service Bus
                var queueConnectionString =
                    CloudConfigurationManager.GetSetting("Uploader.ServiceBus.ConnectionString");
                var queueName = CloudConfigurationManager.GetSetting("Uploader.UploadQueueName");
                s_QueueClient = QueueClient.CreateFromConnectionString(queueConnectionString, queueName);

                // Create the blob client.
                CloudBlobClient blobClient = s_Account.CreateCloudBlobClient();

                // Retrieve a reference to a container. 
                s_Container = blobClient.GetContainerReference(args[0].ToLower());

                // Create the container if it doesn't already exist.
                s_Container.CreateIfNotExists();

                var localFileName = args[1];
                var cloudFileName = args[2];

                //Upload the file
                var uploadedFileName = UploadFile(localFileName, cloudFileName);

                //Notify CAP Server to download file
                SendFileUploadedMessage(uploadedFileName, args[0].ToLower());
            }
            //we don't want to risk crashing on the client machines in deployment
            catch(Exception e)
            {
                Console.WriteLine("Couldn't upload file: " + args[1]  + " check this execption: \n" + e);
            }
        }

        static void PrintUsage()
        {
            Console.WriteLine(@"Uploader.exe [ContainerName] [LocalFilePath] [CloudFilePath]");
            Console.WriteLine("[CloudPath/To/TraceFile.etl.zip");
            Console.WriteLine();
            Console.WriteLine("Uploads the specified file to the CAP data storage for processing. Files are uploaded securly via HTTPS");
            Console.WriteLine("[ContainerName] is case insensitive and cannot have any spaces or punctuation");
            Console.WriteLine("aside from a single hyphen (-)");
            Console.WriteLine("[LocalFilePath] absolute path to local file to upload");
            Console.WriteLine("[CloudFilePath] path");
        }

        static void SendFileUploadedMessage(string fileName, string containerName)
        {
            EtlFileMessage message = new EtlFileMessage()
            {
                FileName = fileName,
                UploadTime = System.DateTime.UtcNow,
                ContainerName = containerName
            };

            //not async this will block but not a big deal
            s_QueueClient.Send(new BrokeredMessage(JsonConvert.SerializeObject(message)));
        }

        static string UploadFile(string path, string cloudPath)
        {
            string convertedPath;
            //tear off any drive letters and switch slashes
            convertedPath = cloudPath;
            Regex r = new Regex(".*:");

            if (r.IsMatch(convertedPath))
            {
                convertedPath = convertedPath.Replace(r.Match(convertedPath).Value, string.Empty);
            }

            //Remove root specifier if exists
            if(convertedPath.StartsWith("/"))
            {
                convertedPath = convertedPath.Substring(1, convertedPath.Length - 1);
            }
            
            //Remove any backslashes and replace with forward ones
            convertedPath = convertedPath.Replace("\\", "/");

            CloudBlockBlob blob = s_Container.GetBlockBlobReference(convertedPath);

            // Create or overwrite the defined blob with contents from a local file.
            using (var fileStream = System.IO.File.OpenRead(path))
            {
                blob.UploadFromStream(fileStream);
            }

            return convertedPath;
        }
    }
}
