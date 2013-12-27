using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.ServiceBus;
using Uploader.SeviceBusMessaging;
using System.Text.RegularExpressions;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System.IO;

namespace Downloader
{
    class Program
    {
        static QueueClient s_QueueClient;

        static CloudStorageAccount s_Account = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("Uploader.ConnectionString"));
        static string s_DownloadRootFolder = CloudConfigurationManager.GetSetting("Downloader.DownloadRootDir");
        static void Main(string[] args)
        {
            Console.CancelKeyPress += Console_CancelKeyPress;

            if(args.Length > 0)
            {
                PrintUsage();
            }

            if(!Directory.Exists(s_DownloadRootFolder))
            {
                Directory.CreateDirectory(s_DownloadRootFolder);
            }

            //Get Service Bus
            var queueConnectionString =
                CloudConfigurationManager.GetSetting("Uploader.ServiceBus.ConnectionString");
            var queueName = CloudConfigurationManager.GetSetting("Uploader.UploadQueueName");
            s_QueueClient = QueueClient.CreateFromConnectionString(queueConnectionString, queueName, ReceiveMode.ReceiveAndDelete);

            CloudBlobClient blobClient = s_Account.CreateCloudBlobClient();

            while (true)
            {
                try
                {
                    var message = s_QueueClient.Receive();

                    //no new messages go back and wait again for more
                    if(message == null)
                    {
                        continue;
                    }

                    FileUploadedMessage etlUploadedMessage = JsonConvert.DeserializeObject<FileUploadedMessage>(message.GetBody<string>());

                    Console.WriteLine("Got uploaded file notification for " + etlUploadedMessage.FileName);

                    // Retrieve a reference to a container. 
                    CloudBlobContainer container = blobClient.GetContainerReference(etlUploadedMessage.ContainerName.ToLower());

                    var blockBlob = container.GetBlockBlobReference(etlUploadedMessage.FileName);
                    var localFilePath = Path.Combine(s_DownloadRootFolder, etlUploadedMessage.ContainerName.ToLower());
                    var filePath = etlUploadedMessage.FileName.Replace("/", "\\");
                    localFilePath = Path.Combine(localFilePath, filePath);

                    if (!Directory.Exists(Path.GetDirectoryName(localFilePath)))
                    {
                        Directory.CreateDirectory(Path.GetDirectoryName(localFilePath));
                    }

                    if (File.Exists(localFilePath))
                    {
                        File.Delete(localFilePath);
                    }

                    //Get the file
                    using (Stream cloudStoredBits = blockBlob.OpenRead())
                    using (FileStream fs = new FileStream(localFilePath, FileMode.CreateNew, FileAccess.ReadWrite))
                    {
                        Console.WriteLine("Downloading Cloud file [" + etlUploadedMessage.ContainerName + "]" + etlUploadedMessage.FileName
                            + " to " + localFilePath);
                        cloudStoredBits.CopyTo(fs);
                    }

                    //Delete it from blob storage. Cloud storage isn't cheap :-)
                    Console.WriteLine("Deleting Cloud file [" + etlUploadedMessage.ContainerName + "]" + etlUploadedMessage.FileName);
                    blockBlob.Delete(DeleteSnapshotsOption.IncludeSnapshots);
                }
                catch(Exception e)
                {
                    Console.WriteLine("Unexpected Exception occured. See this exception - " + e);
                }
            }
        }

        private static void PrintUsage()
        {
            Console.WriteLine("Downloader.exe - Refer to app.config for usage ");
        }

        static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            Environment.Exit(0);
        }
    }
}
